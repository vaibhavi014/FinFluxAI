from __future__ import annotations

from datetime import datetime, timezone

from flask import jsonify


def error_body(status: int, code: str, message: str, correlation_id: str | None):
    return jsonify(
        {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "status": status,
            "error": code,
            "message": message,
            "correlationId": correlation_id,
        }
    ), status


def transaction_view(entity, risk) -> dict:
    return {
        "transactionId": entity.transaction_id,
        "customerId": entity.customer_id,
        "accountId": entity.account_id,
        "amount": str(entity.amount),
        "currency": entity.currency,
        "merchantCategory": entity.merchant_category,
        "location": entity.location,
        "type": entity.type,
        "status": entity.status,
        "timestamp": entity.occurred_at.isoformat() if entity.occurred_at else None,
        "processingStatus": "SCORED" if risk else "PENDING",
        "riskScore": None if risk is None else risk.risk_score,
        "riskLevel": None if risk is None else risk.risk_level,
        "explanation": None if risk is None else risk.explanation,
    }


def risk_view(entity) -> dict:
    return {
        "transactionId": entity.transaction_id,
        "riskScore": entity.risk_score,
        "riskLevel": entity.risk_level,
        "triggeredRules": entity.triggered_rules or [],
        "explanation": entity.explanation,
        "timestamp": entity.assessed_at.isoformat() if entity.assessed_at else None,
    }


def alert_view(entity) -> dict:
    return {
        "transactionId": entity.transaction_id,
        "riskScore": entity.risk_score,
        "riskLevel": entity.risk_level,
        "triggeredRules": entity.triggered_rules or [],
        "explanation": entity.explanation,
        "timestamp": entity.created_at.isoformat() if entity.created_at else None,
    }


def error_view(entity) -> dict:
    return {
        "eventId": entity.event_id,
        "transactionId": entity.transaction_id,
        "errorType": entity.error_type,
        "errorMessage": entity.error_message,
        "retryCount": entity.retry_count,
        "sentToDlq": entity.sent_to_dlq,
        "correlationId": entity.correlation_id,
        "timestamp": entity.created_at.isoformat() if entity.created_at else None,
    }
