import json
import os
import time

import grpc
from logger import log

from core import pipeline
from handler import lake_handler_client, vault_handler_client
from helper import kafka_helper


def _env_flag(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _truncate(s: str, max_chars: int) -> str:
    if max_chars <= 0:
        return s
    if len(s) <= max_chars:
        return s
    return s[:max_chars] + f"... <truncated {len(s) - max_chars} chars>"


def _dump_json(obj, max_chars: int) -> str:
    try:
        s = json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception:
        s = repr(obj)
    return _truncate(s, max_chars)


def _refresh_stubs() -> tuple:
    """Recreate gRPC stubs (helps after transient network/container restarts)."""
    return lake_handler_client.create_stub(), vault_handler_client.create_stub()


def main() -> None:
    log.info("[SEF_CORE] Starting Schema Evolution Framework core")

    debug_payload = _env_flag("SEF_DEBUG_KAFKA_PAYLOAD", default=False)
    debug_trace = _env_flag("SEF_DEBUG_TRACE", default=False)
    max_chars = int(os.getenv("SEF_DEBUG_KAFKA_PAYLOAD_MAX_CHARS", "20000"))

    log.info(
        "[SEF_CORE] Debug flags: SEF_DEBUG_KAFKA_PAYLOAD=%s SEF_DEBUG_TRACE=%s max_chars=%s",
        debug_payload,
        debug_trace,
        max_chars,
    )

    consumer = kafka_helper.create_consumer()
    producer = kafka_helper.create_producer()

    lake_stub, vault_stub = _refresh_stubs()

    for message, notification in kafka_helper.consume_notification(consumer):
        try:
            log.info(
                "[SEF_CORE] Processing notification topic=%s partition=%s offset=%s key=%s",
                getattr(message, "topic", None),
                getattr(message, "partition", None),
                getattr(message, "offset", None),
                getattr(message, "key", None),
            )

            if debug_payload:
                log.info("[SEF_CORE][KAFKA_PAYLOAD] %s", _dump_json(notification, max_chars=max_chars))

            pipeline.process_notification(
                notification=notification,
                lake_stub=lake_stub,
                vault_stub=vault_stub,
                kafka_producer=producer,
            )

            # Commit only this message's offset (avoids skipping failed earlier offsets).
            kafka_helper.commit_offset(consumer, message)

        except Exception as e:
            log.error(
                "[SEF_CORE] Exception while processing notification topic=%s partition=%s offset=%s: %s",
                getattr(message, "topic", None),
                getattr(message, "partition", None),
                getattr(message, "offset", None),
                e,
            )

            # Critical: ensure we retry *the same* offset next, rather than continuing and later
            # committing past it (which would lose the message).
            try:
                kafka_helper.rewind_offset(consumer, message)
            except Exception as seek_exc:
                log.warning("[SEF_CORE] Failed to rewind Kafka offset: %s", seek_exc)

            # If the failure looks like a transient gRPC transport issue, refresh channels.
            try:
                if isinstance(e, grpc.RpcError):
                    code = e.code()
                    if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                        lake_stub, vault_stub = _refresh_stubs()
            except Exception:
                pass

            # Small backoff to avoid tight loops during outages.
            time.sleep(1.0)


if __name__ == "__main__":
    main()
