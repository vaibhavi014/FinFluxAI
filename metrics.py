from __future__ import annotations

from collections import defaultdict
from threading import Lock


class Metrics:
    def __init__(self) -> None:
        self._lock = Lock()
        self._counts: dict[str, int] = defaultdict(int)
        self.last_latency_ms = 0

    def inc(self, name: str) -> None:
        with self._lock:
            self._counts[name] += 1

    def latency(self, millis: int) -> None:
        with self._lock:
            self.last_latency_ms = millis

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "transactionsProcessedTotal": self._counts["processed"],
                "transactionsFailedTotal": self._counts["failed"],
                "transactionsDlqTotal": self._counts["dlq"],
                "transactionsDuplicateTotal": self._counts["duplicate"],
                "highRiskTransactionsTotal": self._counts["high"],
                "lastProcessingLatencyMs": self.last_latency_ms,
            }


metrics = Metrics()
