from logger import log

from core import pipeline
from handler import vault_handler_client, lake_handler_client
from helper import kafka_helper


def main():
    log.info(f"[SEF_CORE] Starting Schema Evolution Framework core")
    consumer = kafka_helper.create_consumer()
    producer = kafka_helper.create_producer()

    lake_stub = lake_handler_client.create_stub()
    vault_stub = vault_handler_client.create_stub()

    for message, notification in kafka_helper.consume_notification(consumer):
        try:
            log.info(f"[SEF_CORE] Processing notification for key={message.key}, offset={message.offset}")
            pipeline.process_notification(
                notification,
                lake_stub,
                vault_stub,
                producer,
            )
            kafka_helper.commit_offset(consumer)
        except Exception as e:
            log.error(f"[SEF_CORE] Exception while processing notification at offset {message.offset}: {e}")
            # No commit -> message will be retried


if __name__ == "__main__":
    main()
