import boto3
import json
import logging
import os
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

class S3Sink:
    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )
        self.bucket = os.getenv("S3_BUCKET", "fraud-detection-lake")

    def _build_s3_key(self, prefix: str) -> str:
        now = datetime.now(timezone.utc)
        return (
            f"{prefix}/"
            f"year={now.year}/month={now.month:02d}/"
            f"day={now.day:02d}/hour={now.hour:02d}/"
            f"batch_{now.strftime('%H%M%S')}.parquet"
        )

    def _to_parquet_bytes(self, records: list[dict]) -> str:
        """Convert list of dicts to a parquet file, return temp file path."""
        flat_records = []
        for r in records:
            flat = {
                "transaction_id":   r["transaction_id"],
                "user_id":          r["user_id"],
                "amount":           r["amount"],
                "currency":         r["currency"],
                "timestamp":        r["timestamp"],
                "country":          r["location"]["country"],
                "city":             r["location"]["city"],
                "lat":              r["location"]["lat"],
                "lon":              r["location"]["lon"],
                "ip_address":       r["location"]["ip_address"],
                "merchant_id":      r["merchant"]["merchant_id"],
                "merchant_name":    r["merchant"]["name"],
                "merchant_category":r["merchant"]["category"],
                "mcc_code":         r["merchant"]["mcc_code"],
                "payment_method":   r["payment_method"],
                "card_last4":       r["card_last4"],
                "device_fingerprint": r["device_fingerprint"],
                "session_id":       r["session_id"],
                "is_fraud":         r.get("is_fraud"),
            }
            flat_records.append(flat)

        table = pa.Table.from_pylist(flat_records)
        tmp   = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        pq.write_table(table, tmp.name)
        return tmp.name

    def flush(self, records: list[dict]):
        if not records:
            return

        s3_key    = self._build_s3_key("raw")
        tmp_path  = self._to_parquet_bytes(records)

        try:
            self.s3.upload_file(tmp_path, self.bucket, s3_key)
            logger.info(f"Flushed {len(records)} records → s3://{self.bucket}/{s3_key}")
        except Exception as e:
            logger.error(f"S3 upload failed: {e}")
            raise
        finally:
            import os as _os
            _os.unlink(tmp_path)