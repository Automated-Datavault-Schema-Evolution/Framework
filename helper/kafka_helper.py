import json
from typing import Any, Dict, Iterable, Tuple

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata, TopicPartition
from logger import log

from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_GROUP_ID,
    KAFKA_TOPIC_NOTIFICATIONS,
    KAFKA_TOPIC_SCHEMA_EVOLVED,
    KAFKA_TOPIC_SCHEMA_FAILED,
)


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC_NOTIFICATIONS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda m: m.decode("utf-8") if m is not None else None,
    )


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
        key_serializer=lambda m: m.encode("utf-8") if m is not None else None,
    )


def consume_notification(consumer: KafkaConsumer) -> Iterable[Tuple[Any, Dict[str, Any]]]:
    while True:
        polled = consumer.poll(timeout_ms=1000, max_records=1)
        if not polled:
            continue
        for _tp, records in polled.items():
            for msg in records or []:
                yield msg, msg.value


def commit_offset(consumer: KafkaConsumer, message: Any) -> None:
    tp = TopicPartition(message.topic, message.partition)
    next_offset = message.offset + 1

    
    try:
        oam = OffsetAndMetadata(next_offset, "")
    except TypeError:
        
        oam = OffsetAndMetadata(next_offset, "", -1)

    consumer.commit({tp: oam})


def rewind_offset(consumer: KafkaConsumer, message: Any) -> None:
    tp = TopicPartition(message.topic, message.partition)
    consumer.seek(tp, message.offset)


def publish_schema_evolved(producer: KafkaProducer, event: Dict[str, Any]):
    key = event["dataset"]["id"]
    try:
        log.info(f"""[SEF_CORE][KAFKA_OUT] topic={KAFKA_TOPIC_SCHEMA_EVOLVED} key={key} event_type={event.get("event_type")} previous_version={event.get("previous_version")} new_version={event.get("new_version")} plan_id={(event.get("plan") or {}).get("plan_id")}""")
    except Exception:
        pass
    producer.send(KAFKA_TOPIC_SCHEMA_EVOLVED, key=key, value=event)
    producer.flush()


def publish_schema_failed(producer: KafkaProducer, event: Dict[str, Any]):
    key = event["dataset"]["id"]
    try:
        log.info(f"""[SEF_CORE][KAFKA_OUT] topic={KAFKA_TOPIC_SCHEMA_FAILED} key={key} event_type={event.get("event_type")} reason={event.get("reason")}""")
    except Exception:
        pass
    producer.send(KAFKA_TOPIC_SCHEMA_FAILED, key=key, value=event)
    producer.flush()
