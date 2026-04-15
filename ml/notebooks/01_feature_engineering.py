# ── CELL 1 — Install dependencies ─────────────────────────────────────────────
%pip install boto3 pyarrow pandas scikit-learn

# ── CELL 2 — Imports and config ───────────────────────────────────────────────
import pandas as pd
import numpy as np
import boto3
import pyarrow.parquet as pq
import pyarrow as pa
import tempfile
import os
from io import BytesIO
from math import radians, sin, cos, sqrt, asin

# ── FILL THESE IN ──────────────────────────────────────────────────────────────
AWS_ACCESS_KEY_ID     = "AKIA2XKHOSRUQMNXR6JV"
AWS_SECRET_ACCESS_KEY = "your_secret_here"
AWS_REGION            = "us-east-1"
S3_BUCKET             = "fraud-detection-lake-737273615465-us-east-1-an"
# ───────────────────────────────────────────────────────────────────────────────

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
)

print("S3 client ready")

# ── CELL 3 — Load all historical parquet files from S3 ────────────────────────
def load_all_parquet(prefix: str) -> pd.DataFrame:
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    files    = [obj["Key"] for obj in response.get("Contents", []) if obj["Key"].endswith(".parquet")]
    print(f"Found {len(files)} parquet files under {prefix}")

    dfs = []
    for key in files:
        obj  = s3.get_object(Bucket=S3_BUCKET, Key=key)
        buf  = BytesIO(obj["Body"].read())
        df   = pd.read_parquet(buf)
        dfs.append(df)
        print(f"  Loaded {key} — {len(df):,} rows")

    combined = pd.concat(dfs, ignore_index=True)
    print(f"\nTotal rows loaded: {len(combined):,}")
    return combined

df = load_all_parquet("raw/historical/")
print(df.head())
print(df.dtypes)

# ── CELL 4 — Basic cleaning ───────────────────────────────────────────────────
df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
df = df.sort_values(["user_id", "timestamp"]).reset_index(drop=True)

df["is_fraud"]      = df["is_fraud"].fillna(False).astype(int)
df["fraud_pattern"] = df["fraud_pattern"].fillna("none")

print(f"Date range: {df['timestamp'].min()} → {df['timestamp'].max()}")
print(f"Fraud rate: {df['is_fraud'].mean()*100:.2f}%")
print(f"Users: {df['user_id'].nunique():,}")
print(f"Merchants: {df['merchant_id'].nunique():,}")

# ── CELL 5 — Haversine distance feature ───────────────────────────────────────
def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a    = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * asin(sqrt(a))

def compute_location_distance(df: pd.DataFrame) -> pd.Series:
    distances = []
    prev      = {}
    for _, row in df.iterrows():
        uid = row["user_id"]
        if uid in prev:
            d = haversine(prev[uid]["lat"], prev[uid]["lon"], row["lat"], row["lon"])
        else:
            d = 0.0
        distances.append(d)
        prev[uid] = {"lat": row["lat"], "lon": row["lon"]}
    return pd.Series(distances, index=df.index)

print("Computing location distances...")
df["location_distance_km"] = compute_location_distance(df)
print("Done")

# ── CELL 6 — Time-based features ──────────────────────────────────────────────
df["hour_of_day"]  = df["timestamp"].dt.hour
df["day_of_week"]  = df["timestamp"].dt.dayofweek
df["is_weekend"]   = (df["day_of_week"] >= 5).astype(int)
df["is_night"]     = ((df["hour_of_day"] >= 22) | (df["hour_of_day"] <= 4)).astype(int)

print("Time features added")
print(df[["hour_of_day", "day_of_week", "is_weekend", "is_night"]].describe())

# ── CELL 7 — Amount features per user ─────────────────────────────────────────
user_stats = df.groupby("user_id")["amount"].agg(
    user_mean_amount="mean",
    user_std_amount="std"
).reset_index()
user_stats["user_std_amount"] = user_stats["user_std_amount"].fillna(1.0)

df = df.merge(user_stats, on="user_id", how="left")

df["amount_zscore"] = (
    (df["amount"] - df["user_mean_amount"]) / df["user_std_amount"]
).fillna(0.0)

df["avg_amount_7d"] = (
    df.groupby("user_id")["amount"]
    .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
)

print("Amount features added")
print(df[["amount", "amount_zscore", "avg_amount_7d"]].describe())

# ── CELL 8 — Velocity features ────────────────────────────────────────────────
df = df.sort_values(["user_id", "timestamp"]).reset_index(drop=True)
df["timestamp_unix"] = df["timestamp"].astype(np.int64) // 10**9

def compute_velocity(df: pd.DataFrame, window_seconds: int) -> pd.Series:
    velocity = []
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

print("Computing velocity features (this takes ~1 min)...")
df["velocity_1h"]  = compute_velocity(df, 3600)
df["velocity_24h"] = compute_velocity(df, 86400)
print("Velocity features added")
print(df[["velocity_1h", "velocity_24h"]].describe())

# ── CELL 9 — Merchant risk score ──────────────────────────────────────────────
merchant_fraud_rate = (
    df.groupby("merchant_category")["is_fraud"]
    .mean()
    .reset_index()
    .rename(columns={"is_fraud": "merchant_risk_score"})
)

df = df.merge(merchant_fraud_rate, on="merchant_category", how="left")
df["merchant_risk_score"] = df["merchant_risk_score"].fillna(0.01)

print("Merchant risk scores:")
print(merchant_fraud_rate.sort_values("merchant_risk_score", ascending=False).to_string())

# ── CELL 10 — New merchant flag ───────────────────────────────────────────────
df = df.sort_values(["user_id", "timestamp"])
df["is_new_merchant"] = (~df.duplicated(subset=["user_id", "merchant_id"], keep="first")).astype(int)
print(f"New merchant transactions: {df['is_new_merchant'].sum():,}")

# ── CELL 11 — Country mismatch ────────────────────────────────────────────────
user_home_country = (
    df.groupby("user_id")["country"]
    .agg(lambda x: x.mode()[0])
    .reset_index()
    .rename(columns={"country": "home_country"})
)

df = df.merge(user_home_country, on="user_id", how="left")
df["country_mismatch"] = (df["country"] != df["home_country"]).astype(int)
print(f"Country mismatch transactions: {df['country_mismatch'].sum():,}")

# ── CELL 12 — Final feature set ───────────────────────────────────────────────
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

TARGET_COL = "is_fraud"

df_features = df[["transaction_id", "user_id", "timestamp"] + FEATURE_COLS + [TARGET_COL, "fraud_pattern"]]

print(f"\nFinal feature dataset shape: {df_features.shape}")
print(f"Features: {FEATURE_COLS}")
print(f"Fraud rate: {df_features[TARGET_COL].mean()*100:.2f}%")
print(df_features[FEATURE_COLS].describe())

#CELL13
def save_to_s3(df: pd.DataFrame, s3_key: str):
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    df.to_parquet(tmp.name, index=False)
    tmp.close()
    s3.upload_file(tmp.name, S3_BUCKET, s3_key)
    os.unlink(tmp.name)
    print(f"Saved → s3://{S3_BUCKET}/{s3_key}")

save_to_s3(df_features, "processed/features/features_v1.parquet")
print("Feature engineering complete")