from __future__ import annotations

import math
import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Deque

from finflux.config import Settings
from finflux.models.cdc import NormalizedTransaction


@dataclass(frozen=True)
class VelocityEvent:
    transaction_id: str
    occurred_at: datetime
    location: str | None
    status: str
    amount: float


@dataclass(frozen=True)
class RiskDecision:
    transaction_id: str
    risk_score: int
    risk_level: str
    triggered_rules: list[str]
    explanation: str
    assessed_at: datetime

    @staticmethod
    def level_for(score: int) -> str:
        if score >= 70:
            return "HIGH"
        if score >= 40:
            return "MEDIUM"
        return "LOW"


class VelocityTracker:
    """In-memory sliding window. One process is enough for this demo."""

    def __init__(self, retain: timedelta | None = None):
        self._retain = retain or timedelta(hours=2)
        self._by_customer: dict[str, Deque[VelocityEvent]] = defaultdict(deque)
        self._lock = threading.Lock()

    def record(self, customer_id: str, event: VelocityEvent) -> list[VelocityEvent]:
        with self._lock:
            events = self._by_customer[customer_id]
            events = deque(e for e in events if e.transaction_id != event.transaction_id)
            events.append(event)
            cutoff = event.occurred_at - self._retain
            while events and events[0].occurred_at < cutoff:
                events.popleft()
            self._by_customer[customer_id] = events
            return list(events)


SENSITIVE_CATEGORIES = {"CRYPTO", "GAMBLING", "MONEY_TRANSFER", "GIFT_CARDS"}


class RiskEngine:
    def __init__(self, settings: Settings, tracker: VelocityTracker | None = None):
        self.settings = settings
        self.tracker = tracker or VelocityTracker()

    def assess(self, tx: NormalizedTransaction) -> RiskDecision:
        occurred = tx.occurred_at
        assert occurred is not None and tx.transaction_id and tx.customer_id and tx.amount is not None
        history = self.tracker.record(
            tx.customer_id,
            VelocityEvent(
                transaction_id=tx.transaction_id,
                occurred_at=occurred,
                location=tx.location,
                status=tx.status or "SUCCESS",
                amount=float(tx.amount),
            ),
        )
        score = 0
        rules: list[str] = []
        reasons: list[str] = []

        if float(tx.amount) >= self.settings.high_value_amount:
            score += 40
            rules.append("HIGH_VALUE_TRANSACTION")
            reasons.append(
                f"amount {tx.amount} {tx.currency} is at or above the high-value threshold of "
                f"{int(self.settings.high_value_amount)}"
            )

        window_start = occurred - timedelta(seconds=self.settings.velocity_window_seconds)
        recent_count = sum(1 for event in history if event.occurred_at >= window_start)
        if recent_count >= self.settings.velocity_threshold:
            score += 40
            rules.append("HIGH_VELOCITY")
            reasons.append(
                f"{recent_count} transactions from this customer in {self.settings.velocity_window_seconds} seconds"
            )

        geo_limit = timedelta(minutes=self.settings.geo_window_minutes)
        location_jump = any(
            event.transaction_id != tx.transaction_id
            and event.location
            and tx.location
            and event.location.lower() != tx.location.lower()
            and abs((event.occurred_at - occurred).total_seconds()) <= geo_limit.total_seconds()
            for event in history
        )
        if location_jump:
            score += 25
            rules.append("GEO_VELOCITY")
            reasons.append(f"customer location changed within {self.settings.geo_window_minutes} minutes")

        category = (tx.merchant_category or "").upper()
        if category in SENSITIVE_CATEGORIES:
            score += 20
            rules.append("UNUSUAL_MERCHANT_CATEGORY")
            reasons.append(f"merchant category {category} is treated as higher risk in this demo")

        failed_start = occurred - timedelta(minutes=self.settings.failed_attempt_window_minutes)
        failed_recent = sum(
            1
            for event in history
            if event.status.upper() == "FAILED" and event.occurred_at >= failed_start
        )
        if failed_recent >= self.settings.failed_attempt_threshold:
            score += 25
            rules.append("REPEATED_FAILED_ATTEMPTS")
            reasons.append(
                f"{failed_recent} failed attempts in {self.settings.failed_attempt_window_minutes} minutes"
            )

        amount = float(tx.amount)
        if amount >= 10_000 and amount % 1000 == 0:
            score += 10
            rules.append("ROUND_AMOUNT_PATTERN")
            reasons.append("amount looks like a round high-value pattern")

        triggered, z_score = self._anomaly(history, amount)
        if triggered:
            score += 20
            rules.append("AMOUNT_ANOMALY")
            reasons.append(
                f"amount is {z_score:.1f} standard deviations above this customer's recent average"
            )

        bounded = min(100, score)
        level = RiskDecision.level_for(bounded)
        return RiskDecision(
            transaction_id=tx.transaction_id,
            risk_score=bounded,
            risk_level=level,
            triggered_rules=list(dict.fromkeys(rules)),
            explanation=_explanation(level, reasons),
            assessed_at=occurred,
        )

    def _anomaly(self, history: list[VelocityEvent], amount: float) -> tuple[bool, float]:
        previous = [event.amount for event in history[:-1]]
        if len(previous) < self.settings.anomaly_min_history:
            return False, 0.0
        mean = sum(previous) / len(previous)
        variance = sum((value - mean) ** 2 for value in previous) / len(previous)
        std = math.sqrt(variance)
        if std < 1:
            std = 1
        z_score = (amount - mean) / std
        return z_score >= self.settings.anomaly_z_score, z_score


def _explanation(level: str, reasons: list[str]) -> str:
    if not reasons:
        return "No risk rules fired. The transaction looks consistent with recent customer activity."
    return f"Risk level {level} because {'; '.join(reasons)}."
