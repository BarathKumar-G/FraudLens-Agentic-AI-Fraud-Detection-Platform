import json
import sys
import os
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from dotenv import load_dotenv

load_dotenv(override=True)

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from transaction_simulator import generate_historical_batch

def upload_to_s3(records: list[dict], batch_num: int):
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )
    bucket = os.getenv("S3_BUCKET")

    flat_records = []
    for r in records:
        flat = {
            "transaction_id":    r["transaction_id"],
            "user_id":           r["user_id"],
            "amount":            r["amount"],
            "currency":          r["currency"],
            "timestamp":         r["timestamp"],
            "country":           r["location"]["country"],
            "city":              r["location"]["city"],
            "lat":               r["location"]["lat"],
            "lon":               r["location"]["lon"],
            "ip_address":        r["location"]["ip_address"],
            "merchant_id":       r["merchant"]["merchant_id"],
            "merchant_name":     r["merchant"]["name"],
            "merchant_category": r["merchant"]["category"],
            "mcc_code":          r["merchant"]["mcc_code"],
            "payment_method":    r["payment_method"],
            "card_last4":        r["card_last4"],
            "device_fingerprint":r["device_fingerprint"],
            "session_id":        r["session_id"],
            "is_fraud":          r.get("is_fraud") or False,
            "fraud_pattern":     r.get("fraud_pattern") or "none",
        }
        flat_records.append(flat)

    table = pa.Table.from_pylist(flat_records)
    tmp   = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    pq.write_table(table, tmp.name)
    tmp.close()

    s3_key = f"raw/historical/batch_{str(batch_num).zfill(4)}.parquet"
    s3.upload_file(tmp.name, bucket, s3_key)
    os.unlink(tmp.name)
    print(f"  Uploaded batch {batch_num} → s3://{bucket}/{s3_key}")

def run(total: int = 100_000, batch_size: int = 10_000):
    print(f"Generating {total:,} historical transactions in batches of {batch_size:,}...")
    transactions = generate_historical_batch(total)

    records = [t.dict() for t in transactions]

    print(f"\nUploading to S3 in {total // batch_size} batches...")
    for i in range(0, len(records), batch_size):
        batch     = records[i:i + batch_size]
        batch_num = i // batch_size + 1
        upload_to_s3(batch, batch_num)

    fraud_count = sum(1 for r in records if r.get("is_fraud"))
    print(f"\nDone.")
    print(f"  Total records : {total:,}")
    print(f"  Fraud records : {fraud_count:,} ({fraud_count/total*100:.2f}%)")
    print(f"  Normal records: {total - fraud_count:,}")
    print(f"  S3 path       : s3://{os.getenv('S3_BUCKET')}/raw/historical/")

if __name__ == "__main__":
    run()