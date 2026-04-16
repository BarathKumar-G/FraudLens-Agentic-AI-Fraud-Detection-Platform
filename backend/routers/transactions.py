from fastapi import APIRouter, HTTPException
from services.s3_service import get_predictions_from_s3

router = APIRouter()

def fetch_predictions(limit=50):
    return get_predictions_from_s3(limit=limit)

@router.get("/alerts")
def get_alerts():
    predictions = fetch_predictions(100)  # slightly larger window for alerts
    alerts = [p for p in predictions if p.get("risk_tier", "").lower() in ["high", "critical"]]
    alerts.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return alerts[:20]

@router.get("/transactions")
def get_transactions():
    predictions = fetch_predictions(50)
    predictions.sort(key=lambda x: x.get("timestamp", ""), reverse=True)
    return predictions

@router.get("/transactions/{id}")
def get_transaction(id: str):
    predictions = fetch_predictions(100)
    for p in predictions:
        if p.get("transaction_id") == id:
            return p
    raise HTTPException(status_code=404, detail="Transaction not found")

@router.get("/metrics")
def get_metrics():
    predictions = fetch_predictions(200)

    total = len(predictions)
    fraud_count = 0
    risk_tiers = {}
    agent_actions = {}

    for p in predictions:
        tier = p.get("risk_tier", "low").lower()
        risk_tiers[tier] = risk_tiers.get(tier, 0) + 1

        if tier in ["high", "critical"]:
            fraud_count += 1

        action = p.get("agent_action", "none").lower()
        agent_actions[action] = agent_actions.get(action, 0) + 1

    fraud_rate = (fraud_count / total * 100) if total > 0 else 0

    return {
        "total_transactions": total,
        "fraud_rate": round(fraud_rate, 2),
        "risk_tiers": risk_tiers,
        "agent_actions": agent_actions
    }