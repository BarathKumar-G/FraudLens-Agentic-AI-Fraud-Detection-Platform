import sys
import os

# Fix import path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import json
import logging
from datetime import datetime, timezone
from io import BytesIO
from collections import defaultdict
from math import radians, sin, cos, sqrt, asin

import boto3
import joblib
import numpy as np
import pandas as pd
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

# ---------------- LOGGING ---------------- #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# -------- ALERT LOGGER -------- #
alert_logger = logging.getLogger("alerts")
alert_logger.setLevel(logging.INFO)

alert_file = f"alerts_{datetime.utcnow().date()}.log"
alert_handler = logging.FileHandler(alert_file)
alert_handler.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
alert_logger.addHandler(alert_handler)

# ---------------- S3 ---------------- #
s3 = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_REGION"),
)

BUCKET = os.getenv("S3_BUCKET")

# ---------------- LOAD MODELS ---------------- #
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

MODEL_DIR = "/app/ml/models"

logger.info(f"Loading models from: {MODEL_DIR}")

rf = joblib.load(os.path.join(MODEL_DIR, "random_forest_v1.pkl"))
scaler = joblib.load(os.path.join(MODEL_DIR, "scaler_v1.pkl"))
imputer = joblib.load(os.path.join(MODEL_DIR, "imputer_v1.pkl"))

logger.info("Models loaded successfully")

# ---------------- AGENT ---------------- #
from agent.agent import run_agent
AGENT_THRESHOLD = 0.6

# ---------------- USER STATE ---------------- #
user_state = defaultdict(lambda: {
    "amounts": [],
    "timestamps": [],
    "last_lat": None,
    "last_lon": None,
    "home_country": None,
})

# ---------------- HAVERSINE ---------------- #
def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return 2 * R * asin(sqrt(a))

# ---------------- FEATURE ENGINE ---------------- #
def create_features(record):
    uid = record["user_id"]
    state = user_state[uid]

    ts = datetime.fromisoformat(record["timestamp"].replace("Z", "").split("+")[0])

    hour = ts.hour
    day = ts.weekday()

    is_weekend = int(day >= 5)
    is_night = int(hour >= 22 or hour <= 4)

    amounts = state["amounts"]

    if len(amounts) > 0:
        mean = np.mean(amounts)
        std = np.std(amounts) if np.std(amounts) > 0 else 1
        zscore = (record["amount"] - mean) / std
        avg_7d = np.mean(amounts[-7:])
    else:
        zscore = 0
        avg_7d = record["amount"]

    now_ts = ts.timestamp()
    timestamps = state["timestamps"]

    velocity_1h = sum(1 for t in timestamps if now_ts - t <= 3600)
    velocity_24h = sum(1 for t in timestamps if now_ts - t <= 86400)

    lat = record["location"]["lat"]
    lon = record["location"]["lon"]

    if state["last_lat"] is not None:
        distance = haversine(state["last_lat"], state["last_lon"], lat, lon)
    else:
        distance = 0

    country = record["location"]["country"]

    if state["home_country"] is None:
        state["home_country"] = country

    country_mismatch = int(country != state["home_country"])

    merchant_cat = record["merchant"]["category"]
    HIGH_RISK = {"crypto", "wire_transfer", "gambling"}
    merchant_risk = 0.9 if merchant_cat in HIGH_RISK else 0.1

    state["amounts"].append(record["amount"])
    state["timestamps"].append(now_ts)
    state["last_lat"] = lat
    state["last_lon"] = lon

    return {
        "amount": record["amount"],
        "amount_zscore": zscore,
        "avg_amount_7d": avg_7d,
        "velocity_1h": velocity_1h,
        "velocity_24h": velocity_24h,
        "location_distance_km": distance,
        "hour_of_day": hour,
        "day_of_week": day,
        "is_weekend": is_weekend,
        "is_night": is_night,
        "merchant_risk_score": merchant_risk,
        "is_new_merchant": 0,
        "country_mismatch": country_mismatch,
    }

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

# ---------------- SAVE ---------------- #
def save_prediction(record):
    now = datetime.now(timezone.utc)

    key = (
        f"predictions/realtime/"
        f"year={now.year}/month={now.month:02d}/"
        f"day={now.day:02d}/hour={now.hour:02d}/"
        f"{record['transaction_id']}.json"
    )

    tmp = BytesIO(json.dumps(record).encode("utf-8"))
    s3.upload_fileobj(tmp, BUCKET, key)

# -------- ALERT UPLOAD -------- #
def upload_alert_logs():
    try:
        s3.upload_file(alert_file, BUCKET, f"logs/alerts/{alert_file}")
    except Exception as e:
        logger.error(f"Alert upload failed: {e}")

# ---------------- KAFKA ---------------- #
KAFKA_CONFIG = {
    "bootstrap.servers": "kafka:9092",
    "group.id": "fraud-realtime",
    "auto.offset.reset": "latest",
}

TOPIC = "transactions"

# ---------------- MAIN ---------------- #
def run():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])

    logger.info("Realtime inference started...")

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            logger.error(msg.error())
            continue

        try:
            record = json.loads(msg.value().decode("utf-8"))

            features = create_features(record)

            X = pd.DataFrame([features])
            X = X.replace([np.inf, -np.inf], np.nan).fillna(0)

            X = imputer.transform(X)
            X = scaler.transform(X)

            prob = rf.predict_proba(X)[0][1]
            record["fraud_probability"] = float(prob)

            # -------- RISK -------- #
            if prob < 0.3:
                record["risk_tier"] = "low"
            elif prob < 0.6:
                record["risk_tier"] = "medium"
            elif prob < 0.85:
                record["risk_tier"] = "high"
            else:
                record["risk_tier"] = "critical"

            # -------- AGENT -------- #
            if prob >= AGENT_THRESHOLD:
                agent_output = run_agent(record)
                record["agent_reason"] = agent_output["reason"]
                record["agent_action"] = agent_output["action"]
            else:
                record["agent_reason"] = "Low risk based on ML model"
                record["agent_action"] = "allow"

            save_prediction(record)

            if record["agent_action"] in ["monitor", "block"]:
                alert_logger.info(
                    f"{record['transaction_id']} | {record['agent_action']} | "
                    f"{record['risk_tier']} | {prob:.3f} | {record['agent_reason']}"
                )
                upload_alert_logs()

            logger.info(
                f"Txn {record['transaction_id']} -> {record['risk_tier']} ({prob:.3f}) | "
                f"Action: {record['agent_action']} | Reason: {record['agent_reason']}"
            )

        except Exception as e:
            logger.error(f"Processing failed: {e}")

if __name__ == "__main__":
    run()