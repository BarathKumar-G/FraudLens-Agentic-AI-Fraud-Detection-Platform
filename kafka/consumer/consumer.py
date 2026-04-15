import json
import logging
from confluent_kafka import Consumer, KafkaError
from s3_sink import S3Sink
import os
from datetime import datetime
import boto3



logger = logging.getLogger(__name__)

if not logger.handlers:
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))

    LOG_FILE = f"consumer_{datetime.utcnow().date()}.log"
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

        s3_key = f"logs/consumer/{LOG_FILE}"

        s3.upload_file(LOG_FILE, bucket, s3_key)

        logger.info(f"Uploaded logs → s3://{bucket}/{s3_key}")

    except Exception as e:
        logger.error(f"Log upload failed: {e}")

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
                    upload_log_to_s3()
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
                upload_log_to_s3()  
                batch = []

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
        if batch:
            sink.flush(batch)
            upload_log_to_s3()
    finally:
        consumer.close()

if __name__ == "__main__":
    run_consumer()