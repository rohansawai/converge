"""Environment-based configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(key: str, default: str | None = None) -> str | None:
    return os.environ.get(key, default)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = "localhost:9092"
    topic_account1: str = "account1-events"
    topic_account2: str = "account2-events"
    topic_account3: str = "account3-events"
    topic_normalized: str = "normalized-events"
    topic_dlq: str = "dead-letter-events"
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "warehouse"
    iceberg_namespace: str = "security"
    iceberg_raw_table: str = "raw_security_events"
    iceberg_state_table: str = "session_current_state"
    session_job_interval_sec: int = 120

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            kafka_bootstrap=_env("KAFKA_BOOTSTRAP", "localhost:9092") or "localhost:9092",
            topic_account1=_env("TOPIC_ACCOUNT1", "account1-events") or "account1-events",
            topic_account2=_env("TOPIC_ACCOUNT2", "account2-events") or "account2-events",
            topic_account3=_env("TOPIC_ACCOUNT3", "account3-events") or "account3-events",
            topic_normalized=_env("TOPIC_NORMALIZED", "normalized-events") or "normalized-events",
            topic_dlq=_env("TOPIC_DLQ", "dead-letter-events") or "dead-letter-events",
            minio_endpoint=_env("MINIO_ENDPOINT", "http://localhost:9000") or "http://localhost:9000",
            minio_access_key=_env("MINIO_ACCESS_KEY", "minioadmin") or "minioadmin",
            minio_secret_key=_env("MINIO_SECRET_KEY", "minioadmin") or "minioadmin",
            minio_bucket=_env("MINIO_BUCKET", "warehouse") or "warehouse",
            iceberg_namespace=_env("ICEBERG_NAMESPACE", "security") or "security",
            iceberg_raw_table=_env("ICEBERG_RAW_TABLE", "raw_security_events") or "raw_security_events",
            iceberg_state_table=_env("ICEBERG_STATE_TABLE", "session_current_state")
            or "session_current_state",
            session_job_interval_sec=int(_env("SESSION_JOB_INTERVAL_SEC", "120") or "120"),
        )


settings = Settings.from_env()
