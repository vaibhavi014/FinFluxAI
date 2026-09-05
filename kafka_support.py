from __future__ import annotations

import json
import logging

from kafka import KafkaProducer

from finflux.config import Settings
from finflux.exceptions import TransientProcessingError

logger = logging.getLogger(__name__)


class KafkaPublisher:
    def __init__(self, settings: Settings):
        self.settings = settings
        self._producer: KafkaProducer | None = None

    def _client(self) -> KafkaProducer:
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self.settings.kafka_bootstrap.split(","),
                acks="all",
                key_serializer=lambda key: None if key is None else str(key).encode("utf-8"),
                value_serializer=lambda value: value.encode("utf-8") if isinstance(value, str) else value,
            )
        return self._producer

    def send(self, topic: str, key: str | None, value: str) -> None:
        try:
            future = self._client().send(topic, key=key, value=value)
            future.get(timeout=10)
        except Exception as ex:
            self._producer = None
            raise TransientProcessingError("KAFKA_UNAVAILABLE", f"Kafka send failed for {topic}") from ex

    def close(self) -> None:
        if self._producer is not None:
            self._producer.flush()
            self._producer.close()
            self._producer = None


def dlq_envelope(original, transaction_id, event_id, error_type, error_message, correlation_id, retries) -> str:
    body = {
        "originalEvent": original.decode("utf-8", errors="replace") if isinstance(original, bytes) else original,
        "transactionId": transaction_id,
        "eventId": event_id,
        "errorType": error_type,
        "errorMessage": error_message,
        "service": "finflux-processor",
        "timestamp": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "correlationId": correlation_id,
        "retryCount": retries,
    }
    return json.dumps(body)
