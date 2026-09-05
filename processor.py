from __future__ import annotations

import json
import logging
from dataclasses import asdict
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy.exc import SQLAlchemyError

from finflux.config import Settings
from finflux.exceptions import TransientProcessingError
from finflux.extensions import db
from finflux.models.cdc import NormalizedTransaction, parse_cdc, validate_transaction
from finflux.models.enrichment import enrich
from finflux.models.entities import Alert, ProcessedEvent, RiskAssessment
from finflux.models.metrics import metrics
from finflux.models.risk import RiskDecision, RiskEngine
from finflux.models.storage import ObjectStore

logger = logging.getLogger(__name__)


class TransactionProcessor:
    def __init__(self, settings: Settings, engine: RiskEngine, store: ObjectStore, publisher):
        self.settings = settings
        self.engine = engine
        self.store = store
        self.publisher = publisher

    def process(self, payload: str, correlation_id: str) -> None:
        parsed = parse_cdc(payload)
        if parsed.operation == "d":
            logger.info("Ignoring delete CDC event eventId=%s", parsed.event_id)
            self._mark_processed(parsed.event_id, parsed.transaction_id, correlation_id)
            metrics.inc("processed")
            return

        if db.session.get(ProcessedEvent, parsed.event_id) is not None:
            logger.info(
                "Duplicate CDC event ignored eventId=%s transactionId=%s",
                parsed.event_id,
                parsed.transaction_id,
            )
            metrics.inc("duplicate")
            return

        tx = validate_transaction(parsed)
        enrichment = enrich(tx, self.settings)
        decision = self.engine.assess(tx)
        self._persist_risk(tx, decision)
        if decision.risk_level != "LOW":
            self._persist_alert(decision)
        self._write_lake(tx, enrichment, decision, correlation_id)
        self._publish(tx, decision, correlation_id)
        self._mark_processed(tx.event_id, tx.transaction_id, correlation_id)
        metrics.inc("processed")
        if decision.risk_level == "HIGH":
            metrics.inc("high")
        logger.info(
            "Processed transaction status=OK riskLevel=%s riskScore=%s rules=%s",
            decision.risk_level,
            decision.risk_score,
            decision.triggered_rules,
        )

    def _persist_risk(self, tx: NormalizedTransaction, decision: RiskDecision) -> None:
        row = RiskAssessment.query.filter_by(transaction_id=tx.transaction_id).one_or_none()
        if row is None:
            row = RiskAssessment(transaction_id=tx.transaction_id)
            db.session.add(row)
        row.event_id = tx.event_id
        row.risk_score = decision.risk_score
        row.risk_level = decision.risk_level
        row.triggered_rules = decision.triggered_rules
        row.explanation = decision.explanation
        row.assessed_at = datetime.now(timezone.utc)

    def _persist_alert(self, decision: RiskDecision) -> None:
        row = Alert.query.filter_by(transaction_id=decision.transaction_id).one_or_none()
        if row is None:
            row = Alert(transaction_id=decision.transaction_id)
            db.session.add(row)
        row.risk_score = decision.risk_score
        row.risk_level = decision.risk_level
        row.triggered_rules = decision.triggered_rules
        row.explanation = decision.explanation

    def _write_lake(self, tx, enrichment, decision: RiskDecision, correlation_id: str) -> None:
        day = (tx.occurred_at or datetime.now(timezone.utc)).date().isoformat()
        file_name = f"{day}/{tx.transaction_id}.json"
        self.store.put("raw", file_name, tx.raw_event)
        processed = {
            "transaction": _tx_dict(tx),
            "enrichment": enrichment,
            "correlationId": correlation_id,
        }
        self.store.put("processed", file_name, json.dumps(processed, default=str))
        self.store.put("curated", file_name, json.dumps(asdict(decision), default=str))
        if decision.risk_level != "LOW":
            self.store.put("alerts", file_name, json.dumps(asdict(decision), default=str))

    def _publish(self, tx, decision: RiskDecision, correlation_id: str) -> None:
        body = {
            "transactionId": tx.transaction_id,
            "customerId": tx.customer_id,
            "riskScore": decision.risk_score,
            "riskLevel": decision.risk_level,
            "triggeredRules": decision.triggered_rules,
            "explanation": decision.explanation,
            "correlationId": correlation_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        payload = json.dumps(body)
        try:
            self.publisher.send(self.settings.processed_topic, tx.transaction_id, payload)
            if decision.risk_level != "LOW":
                self.publisher.send(self.settings.alerts_topic, tx.transaction_id, payload)
        except Exception as ex:
            raise TransientProcessingError("KAFKA_UNAVAILABLE", "Could not publish processed/alert event") from ex

    def _mark_processed(self, event_id: str, transaction_id: str | None, correlation_id: str) -> None:
        if db.session.get(ProcessedEvent, event_id) is None:
            db.session.add(
                ProcessedEvent(
                    event_id=event_id,
                    transaction_id=transaction_id,
                    correlation_id=correlation_id,
                )
            )


def commit_or_transient() -> None:
    try:
        db.session.commit()
    except SQLAlchemyError as ex:
        db.session.rollback()
        raise TransientProcessingError("DATABASE_UNAVAILABLE", "Database is temporarily unavailable") from ex


def _tx_dict(tx: NormalizedTransaction) -> dict:
    data = asdict(tx)
    if isinstance(data.get("amount"), Decimal):
        data["amount"] = str(data["amount"])
    if data.get("occurred_at"):
        data["occurred_at"] = data["occurred_at"].isoformat()
    return data
