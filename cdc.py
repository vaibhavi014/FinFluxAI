from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any

from finflux.exceptions import PermanentProcessingError


@dataclass(frozen=True)
class NormalizedTransaction:
    event_id: str
    operation: str
    transaction_id: str | None
    customer_id: str | None
    account_id: str | None
    amount: Decimal | None
    currency: str | None
    merchant_id: str | None
    merchant_category: str | None
    device_id: str | None
    ip_address: str | None
    location: str | None
    occurred_at: datetime | None
    type: str | None
    status: str | None
    raw_event: str

    def normalized(self) -> "NormalizedTransaction":
        status = (self.status or "SUCCESS").strip().upper()
        return NormalizedTransaction(
            event_id=self.event_id,
            operation=self.operation,
            transaction_id=self.transaction_id.strip() if self.transaction_id else None,
            customer_id=self.customer_id.strip() if self.customer_id else None,
            account_id=self.account_id.strip() if self.account_id else None,
            amount=self.amount,
            currency=self.currency.strip().upper() if self.currency else None,
            merchant_id=_trim(self.merchant_id),
            merchant_category=self.merchant_category.strip().upper() if self.merchant_category else None,
            device_id=_trim(self.device_id),
            ip_address=_trim(self.ip_address),
            location=_trim(self.location),
            occurred_at=self.occurred_at,
            type=self.type.strip().upper() if self.type else None,
            status=status,
            raw_event=self.raw_event,
        )


def _trim(value: str | None) -> str | None:
    if value is None or not value.strip():
        return None
    return value.strip()


def parse_cdc(raw: str) -> NormalizedTransaction:
    import json

    try:
        root = json.loads(raw)
    except json.JSONDecodeError as ex:
        raise PermanentProcessingError("MALFORMED_JSON", "Kafka payload is not valid JSON") from ex
    if not isinstance(root, dict):
        raise PermanentProcessingError("MALFORMED_JSON", "Kafka payload is empty")

    envelope = root.get("payload") if isinstance(root.get("payload"), dict) else root
    op = envelope.get("op")
    if not op:
        raise PermanentProcessingError("MALFORMED_CDC", "CDC envelope is missing op")

    if op == "d":
        before = envelope.get("before") or {}
        txn_id = _text(before, "transaction_id")
        return NormalizedTransaction(
            event_id=_event_id(envelope, txn_id, op),
            operation=op,
            transaction_id=txn_id,
            customer_id=None,
            account_id=None,
            amount=None,
            currency=None,
            merchant_id=None,
            merchant_category=None,
            device_id=None,
            ip_address=None,
            location=None,
            occurred_at=None,
            type=None,
            status=None,
            raw_event=raw,
        )

    after = envelope.get("after")
    if not isinstance(after, dict):
        raise PermanentProcessingError("MALFORMED_CDC", "CDC create/update event is missing after payload")

    txn_id = _text(after, "transaction_id")
    return NormalizedTransaction(
        event_id=_event_id(envelope, txn_id, op),
        operation=op,
        transaction_id=txn_id,
        customer_id=_text(after, "customer_id"),
        account_id=_text(after, "account_id"),
        amount=_decimal(after, "amount"),
        currency=_text(after, "currency"),
        merchant_id=_text(after, "merchant_id"),
        merchant_category=_text(after, "merchant_category"),
        device_id=_text(after, "device_id"),
        ip_address=_text(after, "ip_address"),
        location=_text(after, "location"),
        occurred_at=_timestamp(after, "occurred_at"),
        type=_text(after, "type"),
        status=_text(after, "status"),
        raw_event=raw,
    )


def validate_transaction(tx: NormalizedTransaction) -> NormalizedTransaction:
    if not tx.transaction_id or not tx.transaction_id.strip():
        raise PermanentProcessingError("MISSING_TRANSACTION_ID", "transactionId is required")
    if not tx.customer_id or not tx.customer_id.strip():
        raise PermanentProcessingError("MISSING_CUSTOMER_ID", "customerId is required")
    if not tx.account_id or not tx.account_id.strip():
        raise PermanentProcessingError("MISSING_ACCOUNT_ID", "accountId is required")
    if tx.amount is None:
        raise PermanentProcessingError("INVALID_AMOUNT", "amount is required")
    if tx.amount <= 0:
        raise PermanentProcessingError("INVALID_AMOUNT", "amount must be greater than zero")
    if not tx.currency or not tx.currency.strip():
        raise PermanentProcessingError("MISSING_CURRENCY", "currency is required")
    if tx.occurred_at is None:
        raise PermanentProcessingError("INVALID_TIMESTAMP", "timestamp is required")
    now = datetime.now(timezone.utc)
    occurred = tx.occurred_at if tx.occurred_at.tzinfo else tx.occurred_at.replace(tzinfo=timezone.utc)
    if occurred > now + timedelta(seconds=300):
        raise PermanentProcessingError("INVALID_TIMESTAMP", "timestamp is too far in the future")
    allowed_types = {"PAYMENT", "TRANSFER", "WITHDRAWAL", "REFUND"}
    if not tx.type or tx.type.strip().upper() not in allowed_types:
        raise PermanentProcessingError("INVALID_TYPE", "type must be PAYMENT, TRANSFER, WITHDRAWAL or REFUND")
    if tx.status and tx.status.strip() and tx.status.strip().upper() not in {"SUCCESS", "FAILED", "PENDING"}:
        raise PermanentProcessingError("INVALID_STATUS", "status must be SUCCESS, FAILED or PENDING")
    return tx.normalized()


def _event_id(envelope: dict[str, Any], transaction_id: str | None, op: str) -> str:
    source = envelope.get("source") or {}
    lsn = source.get("lsn", "unknown-lsn")
    tx_id = source.get("txId", "unknown-txid")
    txn = transaction_id or "unknown-txn"
    return f"cdc:{lsn}:{tx_id}:{op}:{txn}"


def _text(node: dict[str, Any], field: str) -> str | None:
    value = node.get(field)
    if value is None:
        return None
    return str(value)


def _decimal(node: dict[str, Any], field: str) -> Decimal | None:
    value = node.get(field)
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as ex:
        raise PermanentProcessingError("INVALID_AMOUNT", f"amount is not a number: {value}") from ex


def _timestamp(node: dict[str, Any], field: str) -> datetime | None:
    value = node.get(field)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        n = int(value)
        if n > 10_000_000_000_000:
            n = n // 1000
        return datetime.fromtimestamp(n / 1000, tz=timezone.utc)
    text = str(value).replace(" ", "T")
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed
    except ValueError as ex:
        raise PermanentProcessingError("INVALID_TIMESTAMP", "occurred_at is not a valid timestamp") from ex
