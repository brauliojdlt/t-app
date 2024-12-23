from pydantic import BaseModel
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column,
    Integer,
    String,
    Float,
    Boolean,
    DateTime,
    JSON,
)

Base = declarative_base()

class Transaction(Base):
    __tablename__ = "transactions"

    transaction_id = Column(String, primary_key=True, index=True)  # Changed to String
    customer_id = Column(String, index=True)  # Changed to String
    card_number = Column(String, index=True)
    timestamp = Column(DateTime, index=True)
    merchant_category = Column(String, index=True)
    merchant_type = Column(String, index=True)
    merchant = Column(String, index=True)
    amount = Column(Float)
    currency = Column(String, index=True)
    country = Column(String, index=True)
    city = Column(String, index=True)
    city_size = Column(String)
    card_type = Column(String)
    card_present = Column(Boolean)
    device = Column(String)
    channel = Column(String)
    device_fingerprint = Column(String)
    ip_address = Column(String)
    distance_from_home = Column(Boolean)
    high_risk_merchant = Column(Boolean)
    transaction_hour = Column(Integer)
    weekend_transaction = Column(Boolean)
    velocity_last_hour = Column(JSON)  # To store the dictionary as a JSON field
    is_fraud = Column(Boolean)