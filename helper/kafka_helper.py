import json
from typing import Dict, Any, Tuple, Iterable

from kafka import KafkaConsumer, KafkaProducer

from config.config import KAFKA_TOPIC_NOTIFICATIONS, KAFKA_BOOTSTRAP_SERVERS, KAFKA_GROUP_ID, KAFKA_TOPIC_SCHEMA_EVOLVED


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
        key_serializer=lambda m: m.encode("utf-8"),
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
    producer.send(KAFKA_TOPIC_SCHEMA_EVOLVED, key=key, value=event)
    producer.flush()
