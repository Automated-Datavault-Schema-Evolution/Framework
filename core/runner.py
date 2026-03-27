import time

import grpc
from logger import log

from config.config import (
    SEF_DEBUG_KAFKA_PAYLOAD,
    SEF_DEBUG_KAFKA_PAYLOAD_MAX_CHARS,
    SEF_DEBUG_TRACE,
)
from core import pipeline
from handler.stub_helper import refresh_stubs
from helper import kafka_helper
from helper.json_helper import dump_json


def run() -> None:
    log.info("[SEF_CORE] Starting Schema Evolution Framework core")

    debug_payload = SEF_DEBUG_KAFKA_PAYLOAD
    max_chars = SEF_DEBUG_KAFKA_PAYLOAD_MAX_CHARS

    log.info(
        "[SEF_CORE] Debug flags: "
        f"SEF_DEBUG_KAFKA_PAYLOAD={debug_payload} "
        f"SEF_DEBUG_TRACE={SEF_DEBUG_TRACE} "
        f"max_chars={max_chars}"
    )

    consumer = kafka_helper.create_consumer()
    producer = kafka_helper.create_producer()
    lake_stub, vault_stub = refresh_stubs()

    for message, notification in kafka_helper.consume_notification(consumer):
        try:
            log.info(
                "[SEF_CORE] Processing notification "
                f"topic={getattr(message, 'topic', None)} "
                f"partition={getattr(message, 'partition', None)} "
                f"offset={getattr(message, 'offset', None)} "
                f"key={getattr(message, 'key', None)}"
            )

            if debug_payload:
                log.info(f"[SEF_CORE][KAFKA_PAYLOAD] {dump_json(notification, max_chars=max_chars)}")

            pipeline.process_notification(
                notification=notification,
                lake_stub=lake_stub,
                vault_stub=vault_stub,
                kafka_producer=producer,
            )
            kafka_helper.commit_offset(consumer, message)
        except Exception as exc:
            log.error(
                "[SEF_CORE] Exception while processing notification "
                f"topic={getattr(message, 'topic', None)} "
                f"partition={getattr(message, 'partition', None)} "
                f"offset={getattr(message, 'offset', None)}: {exc}"
            )

            try:
                kafka_helper.rewind_offset(consumer, message)
            except Exception as seek_exc:
                log.warning(f"[SEF_CORE] Failed to rewind Kafka offset: {seek_exc}")

            try:
                if isinstance(exc, grpc.RpcError):
                    code = exc.code()
                    if code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                        lake_stub, vault_stub = refresh_stubs()
            except Exception:
                pass

            time.sleep(1.0)
