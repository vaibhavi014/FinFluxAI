from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    database_url: str
    kafka_bootstrap: str
    kafka_group: str
    cdc_topic: str
    processed_topic: str
    alerts_topic: str
    dlq_topic: str
    storage_type: str
    storage_path: str
    s3_bucket: str
    s3_region: str
    high_value_amount: float
    velocity_window_seconds: int
    velocity_threshold: int
    geo_window_minutes: int
    failed_attempt_window_minutes: int
    failed_attempt_threshold: int
    anomaly_min_history: int
    anomaly_z_score: float
    enrichment_fail_open: bool
    max_retries: int

    @staticmethod
    def load() -> "Settings":
        user = _env("DATABASE_USER", "finflux")
        password = _env("DATABASE_PASSWORD", "finflux")
        default_url = f"postgresql+psycopg2://{user}:{password}@localhost:5432/finflux"
        return Settings(
            database_url=_env("DATABASE_URL", default_url),
            kafka_bootstrap=_env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            kafka_group=_env("KAFKA_CONSUMER_GROUP", "finflux-processor"),
            cdc_topic=_env("CDC_TOPIC", "finflux.public.transactions"),
            processed_topic=_env("PROCESSED_TOPIC", "transactions.processed"),
            alerts_topic=_env("ALERTS_TOPIC", "transactions.alerts"),
            dlq_topic=_env("DLQ_TOPIC", "transactions.dlq"),
            storage_type=_env("STORAGE_TYPE", "local"),
            storage_path=_env("STORAGE_PATH", "./data/lake"),
            s3_bucket=_env("AWS_S3_BUCKET", ""),
            s3_region=_env("AWS_REGION", "ap-south-1"),
            high_value_amount=float(_env("HIGH_VALUE_AMOUNT", "50000")),
            velocity_window_seconds=int(_env("VELOCITY_WINDOW_SECONDS", "60")),
            velocity_threshold=int(_env("VELOCITY_THRESHOLD", "5")),
            geo_window_minutes=int(_env("GEO_WINDOW_MINUTES", "5")),
            failed_attempt_window_minutes=int(_env("FAILED_ATTEMPT_WINDOW_MINUTES", "10")),
            failed_attempt_threshold=int(_env("FAILED_ATTEMPT_THRESHOLD", "3")),
            anomaly_min_history=int(_env("ANOMALY_MIN_HISTORY", "5")),
            anomaly_z_score=float(_env("ANOMALY_Z_SCORE", "2.5")),
            enrichment_fail_open=_env("ENRICHMENT_FAIL_OPEN", "true").lower() == "true",
            max_retries=int(_env("MAX_RETRIES", "3")),
        )
