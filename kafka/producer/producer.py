import json
import time
import logging
from confluent_kafka import Producer
from transaction_simulator import generate_transaction

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
}

TOPIC = "transactions"

def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Delivered → {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def run_producer(transactions_per_second: int = 2):
    producer = Producer(KAFKA_CONFIG)
    logger.info(f"Producer started — sending {transactions_per_second} tx/sec to topic '{TOPIC}'")

    try:
        while True:
            txn = generate_transaction(fraud_rate=0.08)
            payload = json.dumps(txn.to_kafka_payload())

            producer.produce(
                topic=TOPIC,
                key=txn.user_id,
                value=payload,
                callback=delivery_report,
            )
            producer.poll(0)

            time.sleep(1 / transactions_per_second)

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
    finally:
        producer.flush()
        logger.info("Producer flushed and closed.")

if __name__ == "__main__":
    run_producer()