from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import BigInteger, Boolean, DateTime, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import JSON

from finflux.extensions import db

JsonType = JSON().with_variant(JSONB, "postgresql")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Customer(db.Model):
    __tablename__ = "customers"
    customer_id = db.Column(String(64), primary_key=True)
    full_name = db.Column(String(200), nullable=False)
    city = db.Column(String(100), nullable=False)
    created_at = db.Column(DateTime(timezone=True), nullable=False, default=utcnow)


class Transaction(db.Model):
    __tablename__ = "transactions"
    id = db.Column(BigInteger, primary_key=True, autoincrement=True)
    transaction_id = db.Column(String(64), nullable=False, unique=True)
    customer_id = db.Column(String(64), nullable=False, index=True)
    account_id = db.Column(String(64), nullable=False)
    amount = db.Column(Numeric(18, 2), nullable=False)
    currency = db.Column(String(8), nullable=False, default="INR")
    merchant_id = db.Column(String(64))
    merchant_category = db.Column(String(64))
    device_id = db.Column(String(64))
    ip_address = db.Column(String(64))
    location = db.Column(String(100))
    occurred_at = db.Column(DateTime(timezone=True), nullable=False, index=True)
    type = db.Column(String(32), nullable=False)
    status = db.Column(String(32), nullable=False, default="SUCCESS")
    created_at = db.Column(DateTime(timezone=True), nullable=False, default=utcnow)


class ProcessedEvent(db.Model):
    __tablename__ = "processed_events"
    event_id = db.Column(String(256), primary_key=True)
    transaction_id = db.Column(String(64))
    correlation_id = db.Column(String(64), nullable=False)
    processed_at = db.Column(DateTime(timezone=True), nullable=False, default=utcnow)


class RiskAssessment(db.Model):
    __tablename__ = "risk_assessments"
    id = db.Column(BigInteger, primary_key=True, autoincrement=True)
    transaction_id = db.Column(String(64), nullable=False, unique=True)
    event_id = db.Column(String(256), nullable=False)
    risk_score = db.Column(Integer, nullable=False)
    risk_level = db.Column(String(16), nullable=False, index=True)
    triggered_rules = db.Column(JsonType, nullable=False)
    explanation = db.Column(Text, nullable=False)
    assessed_at = db.Column(DateTime(timezone=True), nullable=False, default=utcnow)


class Alert(db.Model):
    __tablename__ = "alerts"
    id = db.Column(BigInteger, primary_key=True, autoincrement=True)
    transaction_id = db.Column(String(64), nullable=False, unique=True)
    risk_score = db.Column(Integer, nullable=False)
    risk_level = db.Column(String(16), nullable=False)
    triggered_rules = db.Column(JsonType, nullable=False)
    explanation = db.Column(Text, nullable=False)
    created_at = db.Column(DateTime(timezone=True), nullable=False, default=utcnow)


class ProcessingError(db.Model):
    __tablename__ = "processing_errors"
    id = db.Column(BigInteger, primary_key=True, autoincrement=True)
    event_id = db.Column(String(256))
    transaction_id = db.Column(String(64))
    correlation_id = db.Column(String(64), nullable=False)
    error_type = db.Column(String(64), nullable=False)
    error_message = db.Column(Text, nullable=False)
    retry_count = db.Column(Integer, nullable=False, default=0)
    sent_to_dlq = db.Column(Boolean, nullable=False, default=False)
    created_at = db.Column(DateTime(timezone=True), nullable=False, default=utcnow)


def seed_customers() -> None:
    rows = [
        ("CUST-101", "Asha Menon", "Mumbai"),
        ("CUST-102", "Rahul Iyer", "Bengaluru"),
        ("CUST-103", "Neha Kapoor", "Delhi"),
        ("CUST-104", "Vikram Shah", "Pune"),
        ("CUST-105", "Sara Khan", "Hyderabad"),
    ]
    for customer_id, name, city in rows:
        if db.session.get(Customer, customer_id) is None:
            db.session.add(Customer(customer_id=customer_id, full_name=name, city=city))
    db.session.commit()
