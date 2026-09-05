import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from finflux.exceptions import PermanentProcessingError
from finflux.models.cdc import parse_cdc, validate_transaction
from tests.conftest import sample_tx


def test_parse_plain_envelope():
    payload = {
        "op": "c",
        "after": {
            "transaction_id": "TXN-1",
            "customer_id": "CUST-1",
            "account_id": "ACC-1",
            "amount": "120.50",
            "currency": "INR",
            "merchant_category": "GROCERY",
            "location": "Mumbai",
            "occurred_at": "2024-01-01T10:00:00Z",
            "type": "PAYMENT",
            "status": "SUCCESS",
        },
        "source": {"lsn": 99, "txId": 99},
    }
    tx = parse_cdc(json.dumps(payload))
    assert tx.transaction_id == "TXN-1"
    assert tx.amount == Decimal("120.50")
    assert "cdc:99" in tx.event_id


def test_parse_schema_wrapped():
    inner = {"op": "c", "after": {"transaction_id": "TXN-2", "amount": 10}, "source": {"lsn": 1, "txId": 1}}
    tx = parse_cdc(json.dumps({"payload": inner}))
    assert tx.transaction_id == "TXN-2"


def test_malformed_json():
    with pytest.raises(PermanentProcessingError) as caught:
        parse_cdc("{not-json")
    assert caught.value.error_type == "MALFORMED_JSON"


def test_validate_rejects_zero_amount():
    with pytest.raises(PermanentProcessingError) as caught:
        validate_transaction(sample_tx(amount=Decimal("0")))
    assert caught.value.error_type == "INVALID_AMOUNT"


def test_validate_normalizes_type():
    out = validate_transaction(sample_tx(type="payment", currency="inr"))
    assert out.type == "PAYMENT"
    assert out.currency == "INR"
