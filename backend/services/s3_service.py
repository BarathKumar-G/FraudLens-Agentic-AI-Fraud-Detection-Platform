import json
import boto3
from typing import List, Dict, Any
from config import settings

s3_client = boto3.client(
    "s3",
    aws_access_key_id=settings.aws_access_key_id,
    aws_secret_access_key=settings.aws_secret_access_key,
    region_name=settings.aws_region,
)

def get_predictions_from_s3(limit: int = 50) -> List[Dict[str, Any]]:
    prefix = "predictions/realtime/"
    predictions = []

    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=settings.s3_bucket,
            Prefix=prefix
        )

        objects = []
        for page in pages:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if obj["Key"].endswith(".json"):
                        objects.append(obj)

        objects.sort(key=lambda x: x["LastModified"], reverse=True)
        objects = objects[:limit]

        for obj in objects:
            try:
                response = s3_client.get_object(
                    Bucket=settings.s3_bucket,
                    Key=obj["Key"]
                )
                content = response["Body"].read().decode("utf-8")
                predictions.append(json.loads(content))
            except Exception:
                pass

    except Exception:
        pass

    return predictions