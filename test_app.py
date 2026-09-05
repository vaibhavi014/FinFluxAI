import json
from datetime import datetime, timezone

import pytest

from finflux import create_app
from finflux.config import Settings
from finflux.extensions import db
from finflux.models.entities import Alert, ProcessedEvent, RiskAssessment
from finflux.models.processor import TransactionProcessor, commit_or_transient
from finflux.models.risk import RiskEngine, VelocityTracker
from finflux.models.storage import LocalObjectStore


class FakePublisher:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))


@pytest.fixture
def app(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{tmp_path / 'test.db'}")
    monkeypatch.setenv("START_CDC_CONSUMER", "false")
    application = create_app(Settings.load(), start_consumer=False)
    application.config["TESTING"] = True
    return application


@pytest.fixture
def client(app):
    return app.test_client()


def test_rejects_invalid_amount(client):
    response = client.post(
        "/api/v1/transactions",
        json={
            "transactionId": "TXN-X",
            "customerId": "CUST-1",
            "accountId": "ACC-1",
            "amount": 0,
            "currency": "INR",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": "PAYMENT",
        },
    )
    assert response.status_code == 400
    assert response.get_json()["error"] == "VALIDATION_ERROR"


def test_accepts_transaction(client):
    response = client.post(
        "/api/v1/transactions",
        json={
            "transactionId": "TXN-1001",
            "customerId": "CUST-101",
            "accountId": "ACC-1001",
            "amount": 1200,
            "currency": "INR",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": "PAYMENT",
        },
    )
    assert response.status_code == 202
    assert response.get_json()["status"] == "ACCEPTED"


def test_health_and_metrics(client):
    assert client.get("/api/v1/health").status_code == 200
    summary = client.get("/api/v1/metrics/summary").get_json()
    assert "transactions" in summary


def test_dashboard(client):
    assert client.get("/").status_code == 200


def test_processor_duplicate_and_alert(app, tmp_path):
    publisher = FakePublisher()
    with app.app_context():
        processor = TransactionProcessor(
            Settings.load(),
            RiskEngine(Settings.load(), VelocityTracker()),
            LocalObjectStore(str(tmp_path / "lake")),
            publisher,
        )
        payload = json.dumps(
            {
                "op": "c",
                "after": {
                    "transaction_id": "TXN-HV",
                    "customer_id": "CUST-101",
                    "account_id": "ACC-1001",
                    "amount": "95000",
                    "currency": "INR",
                    "merchant_category": "ELECTRONICS",
                    "location": "Mumbai",
                    "occurred_at": datetime.now(timezone.utc).isoformat(),
                    "type": "PAYMENT",
                    "status": "SUCCESS",
                },
                "source": {"lsn": 12, "txId": 12},
            }
        )
        processor.process(payload, "corr-1")
        commit_or_transient()
        processor.process(payload, "corr-2")
        commit_or_transient()
        assert db.session.get(ProcessedEvent, "cdc:12:12:c:TXN-HV") is not None
        assert RiskAssessment.query.filter_by(transaction_id="TXN-HV").count() == 1
        assert Alert.query.filter_by(transaction_id="TXN-HV").count() == 1
        assert len(publisher.sent) == 2
