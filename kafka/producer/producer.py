import json
import time
import logging
from confluent_kafka import Producer
from transaction_simulator import generate_transaction

import os
from datetime import datetime
import boto3
from dotenv import load_dotenv
load_dotenv()
print("S3_BUCKET:", os.getenv("S3_BUCKET"))
logger = logging.getLogger(__name__)

if not logger.handlers:
    logger.setLevel(logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

    # File handler
    LOG_FILE = f"producer_{datetime.utcnow().date()}.log"
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)


def upload_log_to_s3():
    try:
        s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION"),
        )

        bucket = os.getenv("S3_BUCKET")
        s3_key = f"logs/producer/{LOG_FILE}"

        s3.upload_file(LOG_FILE, bucket, s3_key)

        logger.info(f"Uploaded logs -> s3://{bucket}/{s3_key}")

    except Exception as e:
        logger.error(f"Log upload failed: {e}")


# ---------------- KAFKA CONFIG ---------------- #

KAFKA_CONFIG = {"bootstrap.servers": "localhost:9092"}
TOPIC = "transactions"


def delivery_report(err, msg):
    if err:
        logger.error(f"Delivery failed: {err}")
    else:
        logger.info(f"Delivered -> {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def run_producer(transactions_per_second: int = 2):
    producer = Producer(KAFKA_CONFIG)
    logger.info(f"Producer started — sending {transactions_per_second} tx/sec to topic '{TOPIC}'")

    count = 0

    try:
        while True:
            txn = generate_transaction()
            payload = json.dumps(txn.to_kafka_payload())

            producer.produce(
                topic=TOPIC,
                key=txn.user_id,
                value=payload,
                callback=delivery_report,
            )

            producer.poll(0)

            count += 1

            # Upload logs every 20 transactions
            if count % 20 == 0:
                upload_log_to_s3()

            time.sleep(1 / transactions_per_second)

    except KeyboardInterrupt:
        logger.info("Shutting down producer...")
        upload_log_to_s3()

    finally:
        producer.flush()
        logger.info("Producer flushed and closed.")
        upload_log_to_s3()


if __name__ == "__main__":
    run_producer()