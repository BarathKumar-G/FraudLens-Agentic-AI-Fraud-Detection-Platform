import os
import boto3
import joblib
import numpy as np
import pandas as pd
from io import BytesIO
from dotenv import load_dotenv

load_dotenv()

# ---------------- S3 ---------------- #
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

BUCKET = os.getenv("S3_BUCKET")

def load_model(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return joblib.load(BytesIO(obj["Body"].read()))

print("Loading models...")
rf      = load_model("models/random_forest_v1.pkl")
scaler  = load_model("models/scaler_v1.pkl")
imputer = load_model("models/imputer_v1.pkl")
print("Models loaded\n")

# ---------------- FEATURE ORDER ---------------- #
FEATURE_COLS = [
    "amount",
    "amount_zscore",
    "avg_amount_7d",
    "velocity_1h",
    "velocity_24h",
    "location_distance_km",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_night",
    "merchant_risk_score",
    "is_new_merchant",
    "country_mismatch",
]

# ---------------- TEST CASES ---------------- #

tests = [
    {
        "name": "Normal transaction",
        "data": {
            "amount": 50,
            "amount_zscore": 0,
            "avg_amount_7d": 60,
            "velocity_1h": 1,
            "velocity_24h": 3,
            "location_distance_km": 2,
            "hour_of_day": 14,
            "day_of_week": 2,
            "is_weekend": 0,
            "is_night": 0,
            "merchant_risk_score": 0.1,
            "is_new_merchant": 0,
            "country_mismatch": 0,
        }
    },
    {
        "name": "Suspicious transaction",
        "data": {
            "amount": 1500,
            "amount_zscore": 5,
            "avg_amount_7d": 100,
            "velocity_1h": 5,
            "velocity_24h": 20,
            "location_distance_km": 8000,
            "hour_of_day": 3,
            "day_of_week": 6,
            "is_weekend": 1,
            "is_night": 1,
            "merchant_risk_score": 0.9,
            "is_new_merchant": 1,
            "country_mismatch": 1,
        }
    },
    {
        "name": "Extreme fraud case",
        "data": {
            "amount": 5000,
            "amount_zscore": 10,
            "avg_amount_7d": 100,
            "velocity_1h": 10,
            "velocity_24h": 50,
            "location_distance_km": 12000,
            "hour_of_day": 2,
            "day_of_week": 6,
            "is_weekend": 1,
            "is_night": 1,
            "merchant_risk_score": 0.9,
            "is_new_merchant": 1,
            "country_mismatch": 1,
        }
    }
]

# ---------------- RUN TESTS ---------------- #

for test in tests:
    df = pd.DataFrame([test["data"]])[FEATURE_COLS]

    X = df.replace([np.inf, -np.inf], np.nan).fillna(0)
    X = imputer.transform(X)
    X = scaler.transform(X)

    prob = rf.predict_proba(X)[0][1]

    print(f"{test['name']}")
    print(f"Fraud Probability: {prob:.3f}")

    if prob < 0.3:
        tier = "LOW"
    elif prob < 0.6:
        tier = "MEDIUM"
    elif prob < 0.85:
        tier = "HIGH"
    else:
        tier = "CRITICAL"

    print(f"Risk Tier: {tier}")
    print("-" * 40)