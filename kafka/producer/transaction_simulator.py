import random
import uuid
import math
from datetime import datetime, timedelta, timezone
from faker import Faker
from schema import Transaction, Location, Merchant

fake = Faker()
Faker.seed(42)
random.seed(42)

# ── Scale constants ────────────────────────────────────────────────────────────
NUM_USERS           = 5000
NUM_MERCHANTS       = 200
FRAUD_RATE          = 0.40   # 3% — realistic for credit card fraud
HIGH_RISK_RATE      = 0.25   # high-risk merchants have elevated fraud

# ── Cities with realistic fraud risk weights ───────────────────────────────────
CITIES = [
    {"city": "New York",      "country": "US", "lat": 40.7128,  "lon": -74.0060,  "risk": 0.8},
    {"city": "Los Angeles",   "country": "US", "lat": 34.0522,  "lon": -118.2437, "risk": 0.7},
    {"city": "Chicago",       "country": "US", "lat": 41.8781,  "lon": -87.6298,  "risk": 0.7},
    {"city": "Houston",       "country": "US", "lat": 29.7604,  "lon": -95.3698,  "risk": 0.6},
    {"city": "Phoenix",       "country": "US", "lat": 33.4484,  "lon": -112.0740, "risk": 0.5},
    {"city": "Philadelphia",  "country": "US", "lat": 39.9526,  "lon": -75.1652,  "risk": 0.6},
    {"city": "San Antonio",   "country": "US", "lat": 29.4241,  "lon": -98.4936,  "risk": 0.5},
    {"city": "San Diego",     "country": "US", "lat": 32.7157,  "lon": -117.1611, "risk": 0.5},
    {"city": "Dallas",        "country": "US", "lat": 32.7767,  "lon": -96.7970,  "risk": 0.6},
    {"city": "Miami",         "country": "US", "lat": 25.7617,  "lon": -80.1918,  "risk": 0.9},
    {"city": "London",        "country": "GB", "lat": 51.5074,  "lon": -0.1278,   "risk": 1.5},
    {"city": "Paris",         "country": "FR", "lat": 48.8566,  "lon": 2.3522,    "risk": 1.4},
    {"city": "Tokyo",         "country": "JP", "lat": 35.6762,  "lon": 139.6503,  "risk": 1.6},
    {"city": "Sydney",        "country": "AU", "lat": -33.8688, "lon": 151.2093,  "risk": 1.5},
    {"city": "Dubai",         "country": "AE", "lat": 25.2048,  "lon": 55.2708,   "risk": 2.0},
    {"city": "Lagos",         "country": "NG", "lat": 6.5244,   "lon": 3.3792,    "risk": 3.5},
    {"city": "Bucharest",     "country": "RO", "lat": 44.4268,  "lon": 26.1025,   "risk": 3.0},
    {"city": "São Paulo",     "country": "BR", "lat": -23.5505, "lon": -46.6333,  "risk": 2.5},
    {"city": "Moscow",        "country": "RU", "lat": 55.7558,  "lon": 37.6173,   "risk": 2.8},
    {"city": "Bangkok",       "country": "TH", "lat": 13.7563,  "lon": 100.5018,  "risk": 2.2},
]

US_CITIES      = [c for c in CITIES if c["country"] == "US"]
FOREIGN_CITIES = [c for c in CITIES if c["country"] != "US"]

# ── Merchant categories with fraud rate multipliers ───────────────────────────
MERCHANT_CATEGORIES = [
    {"category": "grocery",       "mcc": "5411", "fraud_mult": 0.3,  "avg_amount": 65,   "std": 40},
    {"category": "gas_station",   "mcc": "5541", "fraud_mult": 0.5,  "avg_amount": 55,   "std": 20},
    {"category": "restaurant",    "mcc": "5812", "fraud_mult": 0.4,  "avg_amount": 45,   "std": 30},
    {"category": "pharmacy",      "mcc": "5912", "fraud_mult": 0.4,  "avg_amount": 40,   "std": 25},
    {"category": "clothing",      "mcc": "5651", "fraud_mult": 0.8,  "avg_amount": 120,  "std": 80},
    {"category": "electronics",   "mcc": "5734", "fraud_mult": 2.5,  "avg_amount": 350,  "std": 300},
    {"category": "jewelry",       "mcc": "5944", "fraud_mult": 3.5,  "avg_amount": 800,  "std": 600},
    {"category": "travel",        "mcc": "4722", "fraud_mult": 2.0,  "avg_amount": 600,  "std": 400},
    {"category": "hotel",         "mcc": "7011", "fraud_mult": 1.8,  "avg_amount": 250,  "std": 200},
    {"category": "airline",       "mcc": "3000", "fraud_mult": 2.2,  "avg_amount": 480,  "std": 350},
    {"category": "ecommerce",     "mcc": "5999", "fraud_mult": 3.0,  "avg_amount": 180,  "std": 200},
    {"category": "crypto",        "mcc": "6051", "fraud_mult": 5.0,  "avg_amount": 900,  "std": 800},
    {"category": "wire_transfer", "mcc": "6099", "fraud_mult": 4.5,  "avg_amount": 1200, "std": 900},
    {"category": "atm",           "mcc": "6011", "fraud_mult": 2.8,  "avg_amount": 200,  "std": 150},
    {"category": "entertainment", "mcc": "7999", "fraud_mult": 0.9,  "avg_amount": 80,   "std": 60},
    {"category": "subscription",  "mcc": "5968", "fraud_mult": 0.6,  "avg_amount": 25,   "std": 15},
    {"category": "healthcare",    "mcc": "8099", "fraud_mult": 0.5,  "avg_amount": 150,  "std": 120},
    {"category": "education",     "mcc": "8299", "fraud_mult": 0.3,  "avg_amount": 200,  "std": 150},
    {"category": "insurance",     "mcc": "6300", "fraud_mult": 0.4,  "avg_amount": 300,  "std": 200},
    {"category": "gambling",      "mcc": "7995", "fraud_mult": 4.0,  "avg_amount": 500,  "std": 400},
]

# ── Generate a realistic merchant pool ───────────────────────────────────────
def _build_merchant_pool(n: int) -> list[dict]:
    pool = []
    for i in range(n):
        cat = random.choice(MERCHANT_CATEGORIES)
        pool.append({
            "merchant_id":   f"mrc_{str(i).zfill(4)}",
            "name":          fake.company()[:30],
            "category":      cat["category"],
            "mcc_code":      cat["mcc"],
            "fraud_mult":    cat["fraud_mult"],
            "avg_amount":    cat["avg_amount"],
            "amount_std":    cat["std"],
        })
    return pool

MERCHANTS = _build_merchant_pool(NUM_MERCHANTS)

# ── Generate a realistic user pool ───────────────────────────────────────────
def _build_user_pool(n: int) -> list[dict]:
    pool = []
    for i in range(n):
        home = random.choice(US_CITIES)
        pool.append({
            "user_id":        f"usr_{str(i).zfill(5)}",
            "home_city":      home["city"],
            "home_country":   home["country"],
            "home_lat":       home["lat"],
            "home_lon":       home["lon"],
            "avg_spend":      random.gauss(120, 80),       # personal spending baseline
            "spend_std":      random.uniform(20, 150),
            "risk_profile":   random.choices(             # most users are low risk
                ["low", "medium", "high"],
                weights=[0.75, 0.20, 0.05]
            )[0],
            "account_age_days": random.randint(30, 3650),
        })
    return pool

USERS = _build_user_pool(NUM_USERS)
USER_INDEX = {u["user_id"]: u for u in USERS}

# ── Haversine distance (km) between two lat/lon points ───────────────────────
def _haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

# ── Normal transaction ─────────────────────────────────────────────────────────
def _normal_transaction(user: dict, ts: datetime) -> Transaction:
    merchant = random.choice(MERCHANTS)
    city     = random.choice(US_CITIES)

    # Amount drawn from user's personal baseline, clipped to positive
    raw_amount = random.gauss(user["avg_spend"], user["spend_std"])
    amount     = max(1.0, round(raw_amount, 2))

    return Transaction(
        user_id=user["user_id"],
        amount=amount,
        timestamp=ts.isoformat() + "Z",
        location=Location(
            country=city["country"],
            city=city["city"],
            lat=city["lat"] + random.gauss(0, 0.05),
            lon=city["lon"] + random.gauss(0, 0.05),
            ip_address=fake.ipv4(),
        ),
        merchant=Merchant(
            merchant_id=merchant["merchant_id"],
            name=merchant["name"],
            category=merchant["category"],
            mcc_code=merchant["mcc_code"],
        ),
        payment_method=random.choices(
            ["credit_card", "debit_card", "digital_wallet"],
            weights=[0.5, 0.35, 0.15]
        )[0],
        card_last4=str(random.randint(1000, 9999)),
        device_fingerprint=f"fp_{uuid.uuid4().hex[:8]}",
        session_id=f"sess_{uuid.uuid4().hex[:8]}",
        is_fraud=False,
    )

# ── Fraudulent transaction — 6 distinct fraud patterns ───────────────────────
def _fraudulent_transaction(user: dict, ts: datetime) -> Transaction:
    pattern = random.choices(
        ["foreign_high_value", "velocity_burst", "account_takeover",
         "card_testing",       "merchant_collusion", "unusual_hours"],
        weights=[0.30, 0.20, 0.20, 0.10, 0.10, 0.10]
    )[0]

    city     = random.choice(FOREIGN_CITIES)
    merchant = random.choice([m for m in MERCHANTS if m["fraud_mult"] >= 2.0])

    if pattern == "foreign_high_value":
        # Large charge in a foreign high-risk city
        amount = round(random.uniform(800, 5000), 2)

    elif pattern == "velocity_burst":
        # Many small charges in quick succession — card testing before big charge
        amount = round(random.uniform(1, 15), 2)
        city   = random.choice(US_CITIES)

    elif pattern == "account_takeover":
        # New device, foreign location, large amount
        amount = round(random.uniform(500, 3000), 2)

    elif pattern == "card_testing":
        # Tiny amounts to verify card is live
        amount = round(random.uniform(0.01, 2.00), 2)
        city   = random.choice(US_CITIES)
        merchant = random.choice([m for m in MERCHANTS if m["category"] in ("ecommerce", "subscription")])

    elif pattern == "merchant_collusion":
        # Normal-looking amount but flagged merchant
        amount = round(random.gauss(user["avg_spend"], user["spend_std"] * 0.5), 2)
        amount = max(5.0, amount)
        city   = random.choice(US_CITIES)
        merchant = random.choice([m for m in MERCHANTS if m["category"] in ("crypto", "wire_transfer", "gambling")])

    else:  # unusual_hours — 2 AM – 4 AM transaction
        amount = round(random.uniform(200, 2000), 2)
        ts     = ts.replace(hour=random.randint(2, 4), minute=random.randint(0, 59))

    return Transaction(
        user_id=user["user_id"],
        amount=amount,
        timestamp=ts.isoformat() + "Z",
        location=Location(
            country=city["country"],
            city=city["city"],
            lat=city["lat"] + random.gauss(0, 0.02),
            lon=city["lon"] + random.gauss(0, 0.02),
            ip_address=fake.ipv4(),
        ),
        merchant=Merchant(
            merchant_id=merchant["merchant_id"],
            name=merchant["name"],
            category=merchant["category"],
            mcc_code=merchant["mcc_code"],
        ),
        payment_method=random.choices(
            ["credit_card", "digital_wallet"],
            weights=[0.75, 0.25]
        )[0],
        card_last4=str(random.randint(1000, 9999)),
        device_fingerprint=f"fp_{uuid.uuid4().hex[:8]}",   # new device = red flag
        session_id=f"sess_{uuid.uuid4().hex[:8]}",
        is_fraud=True,
        fraud_pattern=pattern,
    )

# ── Public API ────────────────────────────────────────────────────────────────
def generate_transaction(ts: datetime = None) -> Transaction:
    if ts is None:
        ts = datetime.now(timezone.utc)

    user         = random.choice(USERS)
    base_rate    = FRAUD_RATE

    # Risk profile multiplier
    if user["risk_profile"] == "high":
        base_rate *= 4
    elif user["risk_profile"] == "medium":
        base_rate *= 2

    if random.random() < base_rate:
        return _fraudulent_transaction(user, ts)
    return _normal_transaction(user, ts)


def generate_historical_batch(n: int = 100_000) -> list[Transaction]:
    """
    Generate a large historical dataset for ML training.
    Spreads transactions over the past 90 days so time-based
    features (hour_of_day, day_of_week) have realistic distributions.
    """
    transactions = []
    now          = datetime.now(timezone.utc)

    print(f"Generating {n:,} historical transactions...")
    for i in range(n):
        # Random timestamp within last 90 days
        days_ago    = random.uniform(0, 90)
        hours_ago   = random.gauss(12, 6)          # peak around midday
        hours_ago   = max(0, min(23, hours_ago))
        ts          = now - timedelta(days=days_ago, hours=hours_ago)
        transactions.append(generate_transaction(ts))

        if (i + 1) % 10_000 == 0:
            fraud_count = sum(1 for t in transactions if t.is_fraud)
            print(f"  {i+1:,} generated — fraud so far: {fraud_count} ({fraud_count/(i+1)*100:.2f}%)")

    fraud_total = sum(1 for t in transactions if t.is_fraud)
    print(f"Done. Total: {n:,} | Fraud: {fraud_total:,} ({fraud_total/n*100:.2f}%)")
    return transactions