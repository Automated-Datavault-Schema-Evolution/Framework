import os
import time

import grpc
from logger import log

from core import pipeline
from handler.stub_helper import refresh_stubs
from helper.env_helper import env_flag
from helper.json_helper import dump_json
from helper import kafka_helper


def run() -> None:
    """Run the Schema Evolution Framework core consumer loop.

    Pure orchestration: consume notifications, process via pipeline, commit offsets.
    Functionality preserved from the original main.py.
    """
    log.info("[SEF_CORE] Starting Schema Evolution Framework core")

    debug_payload = env_flag("SEF_DEBUG_KAFKA_PAYLOAD", default=False)
    debug_trace = env_flag("SEF_DEBUG_TRACE", default=False)
    max_chars = int(os.getenv("SEF_DEBUG_KAFKA_PAYLOAD_MAX_CHARS", "20000"))

    log.info(f"""[SEF_CORE] Debug flags: SEF_DEBUG_KAFKA_PAYLOAD={debug_payload} SEF_DEBUG_TRACE={debug_trace} max_chars={max_chars}""")

    consumer = kafka_helper.create_consumer()
    producer = kafka_helper.create_producer()

    lake_stub, vault_stub = refresh_stubs()

    for message, notification in kafka_helper.consume_notification(consumer):
        try:
            log.info(f"""[SEF_CORE] Processing notification topic={getattr(message, "topic", None)} partition={getattr(message, "partition", None)} offset={getattr(message, "offset", None)} key={getattr(message, "key", None)}""")

            if debug_payload:
                log.info(f"""[SEF_CORE][KAFKA_PAYLOAD] {dump_json(notification, max_chars=max_chars)}""")

            # NOTE: debug_trace was present in the original main.py and retained here
            # as a flag for downstream components; it is intentionally not used in
            # this loop to preserve original behavior.
            _ = debug_trace

            pipeline.process_notification(
                notification=notification,
                lake_stub=lake_stub,
                vault_stub=vault_stub,
                kafka_producer=producer,
            )

            # Commit only this message's offset (avoids skipping failed earlier offsets).
            kafka_helper.commit_offset(consumer, message)

        except Exception as e:
            log.error(f"""[SEF_CORE] Exception while processing notification topic={getattr(message, "topic", None)} partition={getattr(message, "partition", None)} offset={getattr(message, "offset", None)}: {e}""")

            # Critical: ensure we retry *the same* offset next, rather than continuing and later
            # committing past it (which would lose the message).
            try:
                kafka_helper.rewind_offset(consumer, message)
            except Exception as seek_exc:
                log.warning(f"""[SEF_CORE] Failed to rewind Kafka offset: {seek_exc}""")

            # If the failure looks like a transient gRPC transport issue, refresh channels.
            try:
                if isinstance(e, grpc.RpcError):
                    code = e.code()
                    if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                        lake_stub, vault_stub = refresh_stubs()
            except Exception:
                pass

            # Small backoff to avoid tight loops during outages.
            time.sleep(1.0)
