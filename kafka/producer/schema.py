from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime
import uuid

class Location(BaseModel):
    country: str
    city: str
    lat: float
    lon: float
    ip_address: str

class Merchant(BaseModel):
    merchant_id: str
    name: str
    category: str
    mcc_code: str

class Transaction(BaseModel):
    transaction_id: str = Field(default_factory=lambda: f"txn_{uuid.uuid4().hex[:12]}")
    user_id: str
    amount: float
    currency: str = "USD"
    timestamp: str = Field(default_factory=lambda: datetime.utcnow().isoformat() + "Z")
    location: Location
    merchant: Merchant
    payment_method: str
    card_last4: str
    device_fingerprint: str
    session_id: str
    is_fraud: Optional[bool] = None
    fraud_pattern: Optional[str] = None

    def to_kafka_payload(self) -> dict:
        return self.dict()       # v1 uses .dict() not .model_dump()

    class Config:
        arbitrary_types_allowed = True