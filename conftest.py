from datetime import datetime, timezone
from decimal import Decimal

from finflux.config import Settings
from finflux.models.cdc import NormalizedTransaction


def settings() -> Settings:
    return Settings.load()


def sample_tx(**overrides) -> NormalizedTransaction:
    data = dict(
        event_id="evt-1",
        operation="c",
        transaction_id="TXN-1",
        customer_id="CUST-1",
        account_id="ACC-1",
        amount=Decimal("10.00"),
        currency="INR",
        merchant_id="M-1",
        merchant_category="GROCERY",
        device_id="D-1",
        ip_address="1.1.1.1",
        location="Mumbai",
        occurred_at=datetime.now(timezone.utc),
        type="PAYMENT",
        status="SUCCESS",
        raw_event="{}",
    )
    data.update(overrides)
    return NormalizedTransaction(**data)
