# ── CELL 1 — Install dependencies ─────────────────────────────────────────────
%pip install boto3 pyarrow pandas scikit-learn imbalanced-learn mlflow

# ── CELL 2 — Imports ──────────────────────────────────────────────────────────
import pandas as pd
import numpy as np
import boto3
import mlflow
import mlflow.sklearn
import tempfile
import os
import json
from io import BytesIO
from sklearn.ensemble import RandomForestClassifier, IsolationForest
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import (
    classification_report,
    roc_auc_score,
    average_precision_score,
    confusion_matrix,
)
from sklearn.preprocessing import StandardScaler
from imblearn.over_sampling import SMOTE

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

# ── CELL 3 — Load features ────────────────────────────────────────────────────
    obj = s3.get_object(Bucket=S3_BUCKET, Key="processed/features/features_v1.parquet")
    df  = pd.read_parquet(BytesIO(obj["Body"].read()))

    print(f"Loaded {len(df):,} rows")
    print(f"Fraud rate: {df['is_fraud'].mean()*100:.2f}%")

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

    X = df[FEATURE_COLS].values
    y = df["is_fraud"].values

# ── CELL 4 — Train/test split ─────────────────────────────────────────────────
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

print(f"Train: {len(X_train):,} | Test: {len(X_test):,}")
print(f"Train fraud: {y_train.sum():,} ({y_train.mean()*100:.2f}%)")
print(f"Test fraud:  {y_test.sum():,}  ({y_test.mean()*100:.2f}%)")

# ── CELL 5 — Handle class imbalance with SMOTE ────────────────────────────────
print("Applying SMOTE to balance training set...")
smote           = SMOTE(random_state=42, sampling_strategy=0.3)
X_train_bal, y_train_bal = smote.fit_resample(X_train, y_train)
print(f"After SMOTE — Train: {len(X_train_bal):,} | Fraud: {y_train_bal.sum():,} ({y_train_bal.mean()*100:.2f}%)")

# ── CELL 6 — Scale features ───────────────────────────────────────────────────
scaler      = StandardScaler()
X_train_sc  = scaler.fit_transform(X_train_bal)
X_test_sc   = scaler.transform(X_test)

# ── CELL 7 — Train Random Forest ──────────────────────────────────────────────
print("\nTraining Random Forest...")
with mlflow.start_run(run_name="random_forest_v1"):
    rf = RandomForestClassifier(
        n_estimators=200,
        max_depth=15,
        min_samples_leaf=5,
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
    )
    rf.fit(X_train_sc, y_train_bal)

    y_pred_rf    = rf.predict(X_test_sc)
    y_proba_rf   = rf.predict_proba(X_test_sc)[:, 1]
    auc_roc      = roc_auc_score(y_test, y_proba_rf)
    auc_pr       = average_precision_score(y_test, y_proba_rf)

    mlflow.log_param("model",         "random_forest")
    mlflow.log_param("n_estimators",  200)
    mlflow.log_param("max_depth",     15)
    mlflow.log_metric("auc_roc",      auc_roc)
    mlflow.log_metric("auc_pr",       auc_pr)
    mlflow.sklearn.log_model(rf, "random_forest_model")

    print(f"Random Forest — AUC-ROC: {auc_roc:.4f} | AUC-PR: {auc_pr:.4f}")
    print(classification_report(y_test, y_pred_rf, target_names=["normal", "fraud"]))

# ── CELL 8 — Train Logistic Regression (baseline) ─────────────────────────────
print("\nTraining Logistic Regression baseline...")
with mlflow.start_run(run_name="logistic_regression_v1"):
    lr = LogisticRegression(
        class_weight="balanced",
        max_iter=1000,
        random_state=42,
    )
    lr.fit(X_train_sc, y_train_bal)

    y_pred_lr   = lr.predict(X_test_sc)
    y_proba_lr  = lr.predict_proba(X_test_sc)[:, 1]
    auc_roc_lr  = roc_auc_score(y_test, y_proba_lr)
    auc_pr_lr   = average_precision_score(y_test, y_proba_lr)

    mlflow.log_param("model",        "logistic_regression")
    mlflow.log_metric("auc_roc",     auc_roc_lr)
    mlflow.log_metric("auc_pr",      auc_pr_lr)
    mlflow.sklearn.log_model(lr, "logistic_regression_model")

    print(f"Logistic Regression — AUC-ROC: {auc_roc_lr:.4f} | AUC-PR: {auc_pr_lr:.4f}")
    print(classification_report(y_test, y_pred_lr, target_names=["normal", "fraud"]))

# ── CELL 9 — Isolation Forest (anomaly detection, unsupervised) ───────────────
print("\nTraining Isolation Forest...")
with mlflow.start_run(run_name="isolation_forest_v1"):
    iso = IsolationForest(
        n_estimators=200,
        contamination=0.012,
        random_state=42,
        n_jobs=-1,
    )
    iso.fit(X_train_sc)

    iso_scores   = iso.decision_function(X_test_sc)
    iso_preds    = (iso.predict(X_test_sc) == -1).astype(int)
    iso_auc      = roc_auc_score(y_test, -iso_scores)

    mlflow.log_param("model",         "isolation_forest")
    mlflow.log_param("contamination", 0.012)
    mlflow.log_metric("auc_roc",      iso_auc)
    mlflow.sklearn.log_model(iso, "isolation_forest_model")

    print(f"Isolation Forest — AUC-ROC: {iso_auc:.4f}")
    print(classification_report(y_test, iso_preds, target_names=["normal", "fraud"]))

# ── CELL 10 — Feature importance ──────────────────────────────────────────────
importance_df = pd.DataFrame({
    "feature":   FEATURE_COLS,
    "importance": rf.feature_importances_
}).sort_values("importance", ascending=False)

print("\nTop features (Random Forest):")
print(importance_df.to_string(index=False))

# ── CELL 11 — Save models and scaler to S3 ────────────────────────────────────
import pickle

def save_pickle_to_s3(obj, s3_key: str):
    tmp = tempfile.NamedTemporaryFile(suffix=".pkl", delete=False)
    with open(tmp.name, "wb") as f:
        pickle.dump(obj, f)
    s3.upload_file(tmp.name, S3_BUCKET, s3_key)
    os.unlink(tmp.name)
    print(f"Saved → s3://{S3_BUCKET}/{s3_key}")

save_pickle_to_s3(rf,      "models/random_forest_v1.pkl")
save_pickle_to_s3(lr,      "models/logistic_regression_v1.pkl")
save_pickle_to_s3(iso,     "models/isolation_forest_v1.pkl")
save_pickle_to_s3(scaler,  "models/scaler_v1.pkl")

metadata = {
    "feature_cols":     FEATURE_COLS,
    "model_version":    "v1",
    "rf_auc_roc":       round(auc_roc, 4),
    "rf_auc_pr":        round(auc_pr, 4),
    "lr_auc_roc":       round(auc_roc_lr, 4),
    "iso_auc_roc":      round(iso_auc, 4),
    "training_samples": len(X_train),
    "test_samples":     len(X_test),
}

tmp = tempfile.NamedTemporaryFile(suffix=".json", mode="w", delete=False)
json.dump(metadata, tmp)
tmp.close()
s3.upload_file(tmp.name, S3_BUCKET, "models/metadata_v1.json")
os.unlink(tmp.name)

print("\nAll models saved to S3")
print(f"Summary: {metadata}")