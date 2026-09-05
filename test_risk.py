from datetime import datetime, timedelta, timezone
from decimal import Decimal

from finflux.config import Settings
from finflux.models.risk import RiskDecision, RiskEngine, VelocityTracker
from tests.conftest import sample_tx


def engine() -> RiskEngine:
    return RiskEngine(Settings.load(), VelocityTracker())


def test_low_risk():
    decision = engine().assess(sample_tx(amount=Decimal("120")))
    assert decision.risk_level == "LOW"
    assert decision.triggered_rules == []


def test_high_value():
    decision = engine().assess(sample_tx(amount=Decimal("95000"), merchant_category="ELECTRONICS"))
    assert "HIGH_VALUE_TRANSACTION" in decision.triggered_rules


def test_velocity_and_high_value_is_high():
    risk = engine()
    now = datetime.now(timezone.utc)
    for i in range(1, 5):
        risk.assess(
            sample_tx(
                transaction_id=f"TXN-C{i}",
                customer_id="CUST-C",
                amount=Decimal("400"),
                occurred_at=now - timedelta(seconds=i),
            )
        )
    decision = risk.assess(
        sample_tx(
            transaction_id="TXN-C5",
            customer_id="CUST-C",
            amount=Decimal("95000"),
            merchant_category="ELECTRONICS",
            occurred_at=now,
        )
    )
    assert "HIGH_VELOCITY" in decision.triggered_rules
    assert "HIGH_VALUE_TRANSACTION" in decision.triggered_rules
    assert decision.risk_level == "HIGH"


def test_score_bands():
    assert RiskDecision.level_for(0) == "LOW"
    assert RiskDecision.level_for(39) == "LOW"
    assert RiskDecision.level_for(40) == "MEDIUM"
    assert RiskDecision.level_for(70) == "HIGH"
