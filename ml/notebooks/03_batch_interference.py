# ── CELL 1 — Install dependencies ─────────────────────────────────────────────
    %pip install boto3 pyarrow pandas scikit-learn

# ── CELL 2 — Imports ──────────────────────────────────────────────────────────
import pandas as pd
import numpy as np
import boto3
import pickle
import tempfile
import os
import json
from io import BytesIO
from datetime import datetime, timezone
from math import radians, sin, cos, sqrt, asin

AWS_ACCESS_KEY_ID     = "AKIA2XKHOSRUQMNXR6JV"
AWS_SECRET_ACCESS_KEY = "your_secret_here"
AWS_REGION            = "us-east-1"
S3_BUCKET             = "fraud-detection-lake-737273615465-us-east-1-an"

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)
print("Ready")

# ── CELL 3 — Load model and scaler from S3 ────────────────────────────────────
def load_pickle(s3_key: str):
    obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
    return pickle.loads(obj["Body"].read())

rf     = load_pickle("models/random_forest_v1.pkl")
scaler = load_pickle("models/scaler_v1.pkl")

obj      = s3.get_object(Bucket=S3_BUCKET, Key="models/metadata_v1.json")
metadata = json.loads(obj["Body"].read())
FEATURE_COLS = metadata["feature_cols"]

print(f"Model loaded: {metadata['model_version']}")
print(f"Features: {FEATURE_COLS}")

# ── CELL 4 — Load raw transactions to score ───────────────────────────────────
def load_parquet_from_prefix(prefix: str) -> pd.DataFrame:
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    files    = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")]
    print(f"Found {len(files)} files under {prefix}")
    dfs = []
    for key in files:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        dfs.append(pd.read_parquet(BytesIO(obj["Body"].read())))
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

df_raw = load_parquet_from_prefix("raw/historical/")
print(f"Loaded {len(df_raw):,} raw transactions")

# ── CELL 5 — Recompute features for inference ─────────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    R    = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a    = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * asin(sqrt(a))

df = df_raw.copy()
df["timestamp"]    = pd.to_datetime(df["timestamp"], utc=True)
df["is_fraud"]     = df["is_fraud"].fillna(False).astype(int)
df                 = df.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
df["hour_of_day"]  = df["timestamp"].dt.hour
df["day_of_week"]  = df["timestamp"].dt.dayofweek
df["is_weekend"]   = (df["day_of_week"] >= 5).astype(int)
df["is_night"]     = ((df["hour_of_day"] >= 22) | (df["hour_of_day"] <= 4)).astype(int)

user_stats = df.groupby("user_id")["amount"].agg(
    user_mean_amount="mean", user_std_amount="std"
).reset_index()
user_stats["user_std_amount"] = user_stats["user_std_amount"].fillna(1.0)
df = df.merge(user_stats, on="user_id", how="left")
df["amount_zscore"] = ((df["amount"] - df["user_mean_amount"]) / df["user_std_amount"]).fillna(0)
df["avg_amount_7d"] = df.groupby("user_id")["amount"].transform(
    lambda x: x.rolling(7, min_periods=1).mean()
)

df["timestamp_unix"] = df["timestamp"].astype(np.int64) // 10**9

def compute_velocity(df, window_seconds):
    velocity   = []
    user_times = {}
    for _, row in df.iterrows():
        uid = row["user_id"]
        t   = row["timestamp_unix"]
        if uid not in user_times:
            user_times[uid] = []
        user_times[uid] = [x for x in user_times[uid] if t - x <= window_seconds]
        velocity.append(len(user_times[uid]))
        user_times[uid].append(t)
    return pd.Series(velocity, index=df.index)

def compute_location_distance(df):
    distances = []
    prev      = {}
    for _, row in df.iterrows():
        uid = row["user_id"]
        d   = haversine(prev[uid]["lat"], prev[uid]["lon"], row["lat"], row["lon"]) if uid in prev else 0.0
        distances.append(d)
        prev[uid] = {"lat": row["lat"], "lon": row["lon"]}
    return pd.Series(distances, index=df.index)

print("Computing velocity and distance features...")
df["velocity_1h"]          = compute_velocity(df, 3600)
df["velocity_24h"]         = compute_velocity(df, 86400)
df["location_distance_km"] = compute_location_distance(df)

merchant_risk = df.groupby("merchant_category")["is_fraud"].mean().reset_index()
merchant_risk.columns = ["merchant_category", "merchant_risk_score"]
df = df.merge(merchant_risk, on="merchant_category", how="left")
df["merchant_risk_score"] = df["merchant_risk_score"].fillna(0.01)

df = df.sort_values(["user_id", "timestamp"])
df["is_new_merchant"] = (~df.duplicated(subset=["user_id", "merchant_id"], keep="first")).astype(int)

user_home = df.groupby("user_id")["country"].agg(lambda x: x.mode()[0]).reset_index()
user_home.columns = ["user_id", "home_country"]
df = df.merge(user_home, on="user_id", how="left")
df["country_mismatch"] = (df["country"] != df["home_country"]).astype(int)

print("Features computed")

# ── CELL 6 — Run inference ────────────────────────────────────────────────────
X      = df[FEATURE_COLS].values
X_sc   = scaler.transform(X)

df["fraud_probability"] = rf.predict_proba(X_sc)[:, 1]
df["fraud_predicted"]   = (df["fraud_probability"] >= 0.5).astype(int)
df["risk_tier"]         = pd.cut(
    df["fraud_probability"],
    bins=[0, 0.3, 0.6, 0.85, 1.0],
    labels=["low", "medium", "high", "critical"]
)
df["model_version"]     = metadata["model_version"]
df["scored_at"]         = datetime.now(timezone.utc).isoformat()

print(f"\nInference complete on {len(df):,} transactions")
print(f"Predicted fraud: {df['fraud_predicted'].sum():,} ({df['fraud_predicted'].mean()*100:.2f}%)")
print(f"\nRisk tier distribution:")
print(df["risk_tier"].value_counts().sort_index())

# ── CELL 7 — Save predictions to S3 ──────────────────────────────────────────
PREDICTION_COLS = [
    "transaction_id",
    "user_id",
    "amount",
    "timestamp",
    "merchant_category",
    "country",
    "fraud_probability",
    "fraud_predicted",
    "risk_tier",
    "model_version",
    "scored_at",
    "is_fraud",         # ground truth — for evaluation
]

df_predictions = df[PREDICTION_COLS].copy()
df_predictions["risk_tier"] = df_predictions["risk_tier"].astype(str)

tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
df_predictions.to_parquet(tmp.name, index=False)
tmp.close()

now     = datetime.now(timezone.utc)
s3_key  = f"predictions/year={now.year}/month={now.month:02d}/day={now.day:02d}/predictions_v1.parquet"
s3.upload_file(tmp.name, S3_BUCKET, s3_key)
os.unlink(tmp.name)

print(f"\nPredictions saved → s3://{S3_BUCKET}/{s3_key}")
print(f"Sample predictions:")
print(df_predictions[["transaction_id", "amount", "fraud_probability", "risk_tier"]].head(10).to_string())