import json
import time
import boto3
from typing import List, Dict, Any
from config import settings

s3_client = boto3.client(
    "s3",
    aws_access_key_id=settings.aws_access_key_id,
    aws_secret_access_key=settings.aws_secret_access_key,
    region_name=settings.aws_region,
)


def get_predictions_from_s3(limit: int = 50):
    prefix = "predictions/realtime/"
    predictions = []

    try:
        paginator = s3_client.get_paginator("list_objects_v2")

        all_objects = []

        for page in paginator.paginate(
            Bucket=settings.s3_bucket,
            Prefix=prefix
        ):
            if "Contents" in page:
                all_objects.extend([
                    obj for obj in page["Contents"]
                    if obj["Key"].endswith(".json")
                ])

        # 🔥 FULL GLOBAL SORT (this is what you were missing)
        all_objects.sort(
            key=lambda x: x["LastModified"],
            reverse=True
        )

        latest_objects = all_objects[:limit]

        for obj in latest_objects:
            try:
                res = s3_client.get_object(
                    Bucket=settings.s3_bucket,
                    Key=obj["Key"]
                )
                content = res["Body"].read().decode("utf-8")
                predictions.append(json.loads(content))
            except:
                continue

    except Exception as e:
        print("S3 ERROR:", e)

    return predictions