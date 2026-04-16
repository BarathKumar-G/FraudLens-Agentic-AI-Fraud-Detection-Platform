# FraudLens: Agentic AI Fraud Detection Platform

## Overview
FraudLens is a real-time fraud detection system that combines:
- Streaming data (Kafka)
- Machine Learning (Random Forest)
- Agent-based reasoning (LLM)
- Cloud storage (AWS S3)
- Backend APIs (FastAPI)
- Frontend dashboard (React + Tailwind)

The system detects fraudulent transactions in real-time and provides explanations using an AI agent.

---

## Architecture

Producer → Kafka → Realtime Inference → S3 → Backend → Frontend

---

## Features

- Real-time transaction simulation
- Fraud prediction using ML model
- Risk classification (LOW, MEDIUM, HIGH, CRITICAL)
- Agent-based reasoning (allow / monitor / flag / block)
- Live dashboard with alerts
- AWS S3 storage for predictions

---

## Project Structure

```
fraudlens/
│
├── kafka/
│   ├── producer/
│   ├── consumer/
│   │   └── realtime_inference.py
│
├── ml/
│   ├── models/
│   └── features/
│
├── backend/
│   ├── main.py
│   ├── config.py
│   ├── routers/
│   └── services/
│
├── frontend/
│   └── src/
│
└── agent/
```

---

## Setup Instructions

### 1. Clone Repository

```
git clone <repo-url>
cd fraudlens
```

---

### 2. Backend Setup

```
cd backend
python -m venv venv
venv\Scripts\activate   # Windows
pip install -r requirements.txt
```

Run backend:
```
uvicorn main:app --reload
```

---

### 3. Frontend Setup

```
cd frontend
npm install
```

Create `.env`:
```
VITE_API_URL=http://localhost:8000
```

Run:
```
npm run dev
```

---

### 4. Kafka Setup

```
docker-compose up
```

---

### 5. Run Pipeline

Terminal 1:
```
python kafka/producer/producer.py
```

Terminal 2:
```
python kafka/consumer/realtime_inference.py
```

---

## Environment Variables

Create `.env`:

```
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_REGION=us-east-1
S3_BUCKET=your_bucket
GROQ_API_KEY=your_key
```

---

## API Endpoints

### Health
GET /health

### Transactions
GET /api/transactions

### Single Transaction
GET /api/transactions/{id}

### Metrics
GET /api/metrics

### Alerts
GET /api/alerts

---

## Data Format

Each transaction includes:

```
{
  "transaction_id": "...",
  "amount": ...,
  "fraud_probability": ...,
  "risk_tier": "...",
  "agent_action": "...",
  "agent_reason": "...",
  "timestamp": "..."
}
```

---

## Risk Levels

- LOW → Safe
- MEDIUM → Suspicious
- HIGH → Risky
- CRITICAL → Fraud

---

## Real-time Behavior

- Producer generates transactions
- Inference processes them
- Results stored in S3
- Backend fetches latest data
- Frontend polls every 2 seconds


```

---

## Troubleshooting

### No data in dashboard
- Ensure producer and inference are running
- Check S3 bucket

### Stuck loading
- Check backend API
- Verify frontend API URL

### No alerts
- Increase fraud rate

---

## Future Improvements

- WebSockets for true real-time
- Pre-aggregated metrics
- User authentication
- Advanced analytics

---
