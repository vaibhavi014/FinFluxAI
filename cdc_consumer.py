from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from sqlalchemy.exc import SQLAlchemyError

from finflux.config import Settings
from finflux.controllers.kafka_support import KafkaPublisher, dlq_envelope
from finflux.exceptions import PermanentProcessingError, TransientProcessingError
from finflux.extensions import db
from finflux.models.entities import ProcessingError
from finflux.models.metrics import metrics
from finflux.models.processor import TransactionProcessor, commit_or_transient

logger = logging.getLogger(__name__)


def ensure_topics(settings: Settings) -> None:
    try:
        admin = KafkaAdminClient(bootstrap_servers=settings.kafka_bootstrap.split(","), client_id="finflux-admin")
        existing = set(admin.list_topics())
        wanted = [
            NewTopic(settings.processed_topic, num_partitions=3, replication_factor=1),
            NewTopic(settings.alerts_topic, num_partitions=3, replication_factor=1),
            NewTopic(settings.dlq_topic, num_partitions=1, replication_factor=1),
        ]
        to_create = [topic for topic in wanted if topic.name not in existing]
        if to_create:
            admin.create_topics(to_create, validate_only=False)
        admin.close()
    except Exception:
        logger.warning("Could not ensure Kafka topics yet; will retry", exc_info=True)


def run_cdc_loop(app, settings: Settings, processor: TransactionProcessor, publisher: KafkaPublisher) -> None:
    while True:
        try:
            ensure_topics(settings)
            consumer = KafkaConsumer(
                settings.cdc_topic,
                bootstrap_servers=settings.kafka_bootstrap.split(","),
                group_id=settings.kafka_group,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                key_deserializer=lambda key: None if key is None else key.decode("utf-8"),
                value_deserializer=lambda value: value.decode("utf-8") if value else "",
            )
            logger.info("CDC consumer subscribed to %s", settings.cdc_topic)
            for message in consumer:
                _handle(app, settings, processor, publisher, consumer, message)
        except Exception:
            logger.exception("CDC consumer loop failed; retrying in 5s")
            time.sleep(5)


def _handle(app, settings, processor, publisher, consumer, message) -> None:
    started = time.time()
    correlation_id = str(uuid.uuid4())
    payload = message.value or ""
    retries = 0
    with app.app_context():
        while True:
            try:
                processor.process(payload, correlation_id)
                commit_or_transient()
                consumer.commit()
                metrics.latency(int((time.time() - started) * 1000))
                return
            except PermanentProcessingError as ex:
                metrics.inc("failed")
                logger.error("Permanent processing failure errorType=%s message=%s", ex.error_type, ex)
                _to_dlq(settings, processor, publisher, consumer, message, ex, correlation_id, retries)
                return
            except TransientProcessingError as ex:
                metrics.inc("failed")
                db.session.rollback()
                retries += 1
                logger.warning("Transient processing failure errorType=%s retry=%s", ex.error_type, retries)
                if retries > settings.max_retries:
                    _to_dlq(settings, processor, publisher, consumer, message, ex, correlation_id, retries)
                    return
                time.sleep(min(4, 2 ** (retries - 1)))
            except SQLAlchemyError as ex:
                db.session.rollback()
                wrapped = TransientProcessingError("DATABASE_UNAVAILABLE", "Database is temporarily unavailable")
                wrapped.__cause__ = ex
                retries += 1
                if retries > settings.max_retries:
                    _to_dlq(settings, processor, publisher, consumer, message, wrapped, correlation_id, retries)
                    return
                time.sleep(min(4, 2 ** (retries - 1)))
            except Exception as ex:
                db.session.rollback()
                wrapped = TransientProcessingError("PROCESSING_EXCEPTION", "Unexpected processing error")
                wrapped.__cause__ = ex
                retries += 1
                logger.exception("Unexpected processing error")
                if retries > settings.max_retries:
                    _to_dlq(settings, processor, publisher, consumer, message, wrapped, correlation_id, retries)
                    return
                time.sleep(min(4, 2 ** (retries - 1)))


def _to_dlq(settings, processor, publisher, consumer, message, error, correlation_id, retries) -> None:
    error_type = getattr(error, "error_type", error.__class__.__name__)
    envelope = dlq_envelope(
        original=message.value,
        transaction_id=None,
        event_id=None,
        error_type=error_type,
        error_message=str(error),
        correlation_id=correlation_id,
        retries=retries,
    )
    try:
        publisher.send(settings.dlq_topic, message.key, envelope)
    except Exception:
        logger.exception("Failed to publish DLQ envelope")
    try:
        db.session.add(
            ProcessingError(
                event_id=None,
                transaction_id=None,
                correlation_id=correlation_id or "unknown",
                error_type=error_type,
                error_message=str(error),
                retry_count=retries,
                sent_to_dlq=True,
                created_at=datetime.now(timezone.utc),
            )
        )
        db.session.commit()
    except Exception:
        db.session.rollback()
        logger.warning("Could not persist processing_errors row")
    metrics.inc("dlq")
    consumer.commit()
    logger.error("Event sent to DLQ errorType=%s", error_type)
