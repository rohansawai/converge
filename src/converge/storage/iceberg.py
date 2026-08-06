"""PyIceberg catalog and table management."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    UUIDType,
)

from converge.config import settings

CATALOG_DB_PATH = Path(os.environ.get("ICEBERG_CATALOG_PATH", "/tmp/converge_iceberg_catalog.db"))


def _warehouse_uri() -> str:
    return f"s3://{settings.minio_bucket}/"


def _catalog_config() -> dict[str, str]:
    endpoint = settings.minio_endpoint.replace("http://", "").replace("https://", "")
    return {
        "type": "sql",
        "uri": f"sqlite:///{CATALOG_DB_PATH}",
        "warehouse": _warehouse_uri(),
        "s3.endpoint": f"http://{endpoint}",
        "s3.access-key-id": settings.minio_access_key,
        "s3.secret-access-key": settings.minio_secret_key,
        "s3.path-style-access": "true",
    }


def get_catalog():
    CATALOG_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return load_catalog("converge", **_catalog_config())


RAW_SCHEMA = Schema(
    NestedField(1, "event_id", UUIDType(), required=True),
    NestedField(2, "source_account", StringType(), required=True),
    NestedField(3, "event_type", StringType(), required=True),
    NestedField(4, "occurred_at", TimestampType(), required=True),
    NestedField(5, "actor", StringType()),
    NestedField(6, "target", StringType()),
    NestedField(7, "outcome", StringType(), required=True),
    NestedField(8, "severity", StringType(), required=True),
    NestedField(9, "session_id", StringType()),
    NestedField(10, "flags", ListType(11, StringType(), element_required=True)),
    NestedField(12, "raw_payload", StringType(), required=True),
)

STATE_SCHEMA = Schema(
    NestedField(1, "session_id", StringType(), required=True),
    NestedField(2, "actor", StringType()),
    NestedField(3, "last_seen", TimestampType(), required=True),
    NestedField(4, "failed_login_count", LongType(), required=True),
    NestedField(5, "flagged", BooleanType(), required=True),
    NestedField(6, "risk_score", DoubleType(), required=True),
    NestedField(7, "event_count", LongType(), required=True),
)


def ensure_tables() -> tuple[Any, Any]:
    catalog = get_catalog()
    if settings.iceberg_namespace not in [t[0] for t in catalog.list_namespaces()]:
        catalog.create_namespace(settings.iceberg_namespace)

    raw_identifier = f"{settings.iceberg_namespace}.{settings.iceberg_raw_table}"
    state_identifier = f"{settings.iceberg_namespace}.{settings.iceberg_state_table}"

    if not catalog.table_exists(raw_identifier):
        catalog.create_table(
            raw_identifier,
            schema=RAW_SCHEMA,
            partition_spec=None,
        )

    if not catalog.table_exists(state_identifier):
        catalog.create_table(
            state_identifier,
            schema=STATE_SCHEMA,
            partition_spec=None,
        )

    return catalog.load_table(raw_identifier), catalog.load_table(state_identifier)


def security_event_to_arrow(events: list[dict[str, Any]]) -> pa.Table:
    import json
    from datetime import datetime
    from uuid import UUID

    rows = {
        "event_id": [],
        "source_account": [],
        "event_type": [],
        "occurred_at": [],
        "actor": [],
        "target": [],
        "outcome": [],
        "severity": [],
        "session_id": [],
        "flags": [],
        "raw_payload": [],
    }
    for event in events:
        rows["event_id"].append(UUID(str(event["event_id"])))
        rows["source_account"].append(event["source_account"])
        rows["event_type"].append(event["event_type"])
        occurred = event["occurred_at"]
        if isinstance(occurred, str):
            occurred = datetime.fromisoformat(occurred.replace("Z", "+00:00"))
        rows["occurred_at"].append(occurred)
        rows["actor"].append(event.get("actor"))
        rows["target"].append(event.get("target"))
        rows["outcome"].append(event["outcome"])
        rows["severity"].append(event["severity"])
        rows["session_id"].append(event.get("session_id"))
        rows["flags"].append(event.get("flags") or [])
        rows["raw_payload"].append(json.dumps(event.get("raw_payload", event)))

    return pa.table(rows)


def state_rows_to_arrow(rows: list[dict[str, Any]]) -> pa.Table:
    from datetime import datetime

    data = {
        "session_id": [],
        "actor": [],
        "last_seen": [],
        "failed_login_count": [],
        "flagged": [],
        "risk_score": [],
        "event_count": [],
    }
    for row in rows:
        data["session_id"].append(row["session_id"])
        data["actor"].append(row.get("actor"))
        last_seen = row["last_seen"]
        if isinstance(last_seen, str):
            last_seen = datetime.fromisoformat(last_seen.replace("Z", "+00:00"))
        data["last_seen"].append(last_seen)
        data["failed_login_count"].append(int(row["failed_login_count"]))
        data["flagged"].append(bool(row["flagged"]))
        data["risk_score"].append(float(row["risk_score"]))
        data["event_count"].append(int(row["event_count"]))
    return pa.table(data)
