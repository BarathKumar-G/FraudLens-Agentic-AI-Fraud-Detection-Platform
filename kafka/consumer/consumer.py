import json
import logging
from confluent_kafka import Consumer, KafkaError
from s3_sink import S3Sink

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id":          "fraud-detection-consumer",
    "auto.offset.reset": "earliest",
}

TOPIC       = "transactions"
BATCH_SIZE  = 20
BATCH_TIMEOUT = 30

def run_consumer():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])
    sink = S3Sink()

    batch  = []
    logger.info(f"Consumer listening on topic '{TOPIC}'...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                if batch:
                    sink.flush(batch)
                    batch = []
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                logger.error(f"Kafka error: {msg.error()}")
                continue

            record = json.loads(msg.value().decode("utf-8"))
            batch.append(record)
            logger.info(f"Consumed txn {record['transaction_id']} | user={record['user_id']} | amount={record['amount']}")

            if len(batch) >= BATCH_SIZE:
                sink.flush(batch)
                batch = []

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
        if batch:
            sink.flush(batch)
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()