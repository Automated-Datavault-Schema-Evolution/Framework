import json
from typing import Dict, Any, Tuple, Iterable

from kafka import KafkaConsumer, KafkaProducer
from logger import log

from config.config import KAFKA_TOPIC_NOTIFICATIONS, KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, \
    KAFKA_TOPIC_SCHEMA_EVOLVED, KAFKA_TOPIC_SCHEMA_FAILED


def create_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC_NOTIFICATIONS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda m: m.decode("utf-8") if m is not None else None
    )


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
        key_serializer=lambda m: m.encode("utf-8") if m is not None else None
    )


def consume_notification(consumer: KafkaConsumer) -> Iterable[Tuple[Any, Dict[str, Any]]]:
    """
    Yield (raw_message, notification_dict) pairs
    """
    for msg in consumer:
        yield msg, msg.value


def commit_offset(consumer: KafkaConsumer):
    consumer.commit()


def publish_schema_evolved(producer: KafkaProducer, event: Dict[str, Any]):
    key = event["dataset"]["id"]
    try:
        log.info(
            "[SEF_CORE][KAFKA_OUT] topic=%s key=%s event_type=%s previous_version=%s new_version=%s plan_id=%s",
            KAFKA_TOPIC_SCHEMA_EVOLVED,
            key,
            event.get("event_type"),
            event.get("previous_version"),
            event.get("new_version"),
            (event.get("plan") or {}).get("plan_id"),
        )
    except Exception:
        pass
    producer.send(KAFKA_TOPIC_SCHEMA_EVOLVED, key=key, value=event)
    producer.flush()


def publish_schema_failed(producer: KafkaProducer, event: Dict[str, Any]):
    key = event["dataset"]["id"]
    try:
        log.info(
            "[SEF_CORE][KAFKA_OUT] topic=%s key=%s event_type=%s reason=%s",
            KAFKA_TOPIC_SCHEMA_FAILED,
            key,
            event.get("event_type"),
            event.get("reason"),
        )
    except Exception:
        pass
    producer.send(KAFKA_TOPIC_SCHEMA_FAILED, key=key, value=event)
    producer.flush()
