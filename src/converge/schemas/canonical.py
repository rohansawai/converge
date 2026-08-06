"""Canonical security event schema."""

from __future__ import annotations

import json
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


class EventType(str, Enum):
    LOGIN = "login"
    API_CALL = "api_call"
    NETWORK = "network"
    UNKNOWN = "unknown"


class Outcome(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    UNKNOWN = "unknown"


class Severity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class SecurityEvent(BaseModel):
    event_id: UUID = Field(default_factory=uuid4)
    source_account: str
    event_type: EventType
    occurred_at: datetime
    actor: str | None = None
    target: str | None = None
    outcome: Outcome
    severity: Severity
    session_id: str | None = None
    flags: list[str] = Field(default_factory=list)
    raw_payload: dict[str, Any]

    def to_json_bytes(self) -> bytes:
        return self.model_dump_json().encode("utf-8")

    @classmethod
    def from_json_bytes(cls, data: bytes) -> SecurityEvent:
        return cls.model_validate_json(data)


class DeadLetterRecord(BaseModel):
    source_topic: str
    error: str
    raw_payload: dict[str, Any] | str
    failed_at: datetime = Field(default_factory=lambda: datetime.utcnow())

    def to_json_bytes(self) -> bytes:
        payload = self.model_dump(mode="json")
        return json.dumps(payload, default=str).encode("utf-8")
