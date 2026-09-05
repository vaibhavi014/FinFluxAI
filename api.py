from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

from flask import Blueprint, g, request
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from finflux.extensions import db
from finflux.models.entities import Alert, ProcessingError, RiskAssessment, Transaction
from finflux.models.metrics import metrics
from finflux.views.json_views import alert_view, error_body, error_view, risk_view, transaction_view

api_bp = Blueprint("api", __name__, url_prefix="/api/v1")

ALLOWED_TYPES = {"PAYMENT", "TRANSFER", "WITHDRAWAL", "REFUND"}
ALLOWED_STATUSES = {"SUCCESS", "FAILED", "PENDING"}


@api_bp.post("/transactions")
def create_transaction():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return error_body(400, "MALFORMED_JSON", "Request body is not valid JSON", g.correlation_id)
    required = ["transactionId", "customerId", "accountId", "amount", "currency", "timestamp", "type"]
    missing = [field for field in required if not body.get(field)]
    if missing:
        return error_body(400, "VALIDATION_ERROR", f"missing fields: {', '.join(missing)}", g.correlation_id)
    try:
        amount = Decimal(str(body["amount"]))
    except (InvalidOperation, ValueError):
        return error_body(400, "VALIDATION_ERROR", "amount must be greater than zero", g.correlation_id)
    if amount <= 0:
        return error_body(400, "VALIDATION_ERROR", "amount must be greater than zero", g.correlation_id)
    txn_type = str(body["type"]).strip().upper()
    if txn_type not in ALLOWED_TYPES:
        return error_body(400, "VALIDATION_ERROR", "type must be PAYMENT, TRANSFER, WITHDRAWAL or REFUND", g.correlation_id)
    status = str(body.get("status") or "SUCCESS").strip().upper()
    if status not in ALLOWED_STATUSES:
        return error_body(400, "VALIDATION_ERROR", "status must be SUCCESS, FAILED or PENDING", g.correlation_id)
    try:
        occurred = datetime.fromisoformat(str(body["timestamp"]).replace("Z", "+00:00"))
        if occurred.tzinfo is None:
            occurred = occurred.replace(tzinfo=timezone.utc)
    except ValueError:
        return error_body(400, "VALIDATION_ERROR", "timestamp is not a valid ISO-8601 datetime", g.correlation_id)

    entity = Transaction(
        transaction_id=str(body["transactionId"]).strip(),
        customer_id=str(body["customerId"]).strip(),
        account_id=str(body["accountId"]).strip(),
        amount=amount,
        currency=str(body["currency"]).strip().upper(),
        merchant_id=_blank(body.get("merchantId")),
        merchant_category=_blank(body.get("merchantCategory")),
        device_id=_blank(body.get("deviceId")),
        ip_address=_blank(body.get("ipAddress")),
        location=_blank(body.get("location")),
        occurred_at=occurred,
        type=txn_type,
        status=status,
    )
    db.session.add(entity)
    try:
        db.session.commit()
    except IntegrityError:
        db.session.rollback()
        return error_body(409, "REQUEST_ERROR", f"transactionId already exists: {entity.transaction_id}", g.correlation_id)
    except SQLAlchemyError:
        db.session.rollback()
        return error_body(503, "DATABASE_UNAVAILABLE", "Database is temporarily unavailable", g.correlation_id)
    return {
        "transactionId": entity.transaction_id,
        "status": "ACCEPTED",
        "message": "Stored in PostgreSQL. Debezium will publish a CDC event for scoring.",
    }, 202


@api_bp.get("/transactions/<txn_id>")
def get_transaction(txn_id: str):
    entity = Transaction.query.filter_by(transaction_id=txn_id).one_or_none()
    if entity is None:
        return error_body(404, "NOT_FOUND", f"transaction not found: {txn_id}", g.correlation_id)
    risk = RiskAssessment.query.filter_by(transaction_id=txn_id).one_or_none()
    return transaction_view(entity, risk)


@api_bp.get("/transactions")
def list_transactions():
    rows = Transaction.query.order_by(Transaction.occurred_at.desc()).limit(50).all()
    risks = {row.transaction_id: row for row in RiskAssessment.query.all()}
    return [transaction_view(row, risks.get(row.transaction_id)) for row in rows]


@api_bp.get("/risk/<txn_id>")
def get_risk(txn_id: str):
    entity = RiskAssessment.query.filter_by(transaction_id=txn_id).one_or_none()
    if entity is None:
        return error_body(404, "NOT_FOUND", f"risk assessment not ready yet for {txn_id}", g.correlation_id)
    return risk_view(entity)


@api_bp.get("/alerts")
def list_alerts():
    rows = Alert.query.order_by(Alert.created_at.desc()).limit(50).all()
    return [alert_view(row) for row in rows]


@api_bp.get("/errors")
def list_errors():
    rows = ProcessingError.query.order_by(ProcessingError.created_at.desc()).limit(50).all()
    return [error_view(row) for row in rows]


@api_bp.get("/health")
def health():
    return {"status": "UP", "service": "finflux-app"}


@api_bp.get("/metrics/summary")
def metrics_summary():
    return {
        "transactions": {
            "total": Transaction.query.count(),
            "low": RiskAssessment.query.filter_by(risk_level="LOW").count(),
            "medium": RiskAssessment.query.filter_by(risk_level="MEDIUM").count(),
            "high": RiskAssessment.query.filter_by(risk_level="HIGH").count(),
        },
        "pipeline": metrics.snapshot(),
        "alerts": Alert.query.count(),
        "dlqRows": ProcessingError.query.count(),
    }


def _blank(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None
