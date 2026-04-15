import boto3
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "../../.env"))

BASE_DIR = os.path.dirname(__file__)

s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

bucket = os.getenv("S3_BUCKET")

files = [
    "models/random_forest_v1.pkl",
    "models/scaler_v1.pkl",
    "models/imputer_v1.pkl"
]

for f in files:
    filename = f.split("/")[-1]
    local_path = os.path.join(BASE_DIR, filename)

    print(f"Downloading {f} → {local_path}")
    s3.download_file(bucket, f, local_path)

print("Done ✅")