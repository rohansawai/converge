"""Normalizer registry and shared helpers."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Callable

from pydantic import ValidationError

from converge.schemas.canonical import EventType, Outcome, SecurityEvent, Severity
from converge.schemas.sources import Account1Event, Account2Event, Account3Event

NormalizerFn = Callable[[dict[str, Any], str], SecurityEvent]


def parse_timestamp(value: str) -> datetime:
    normalized = value.replace("Z", "+00:00")
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def derive_session_id(actor: str | None, occurred_at: datetime) -> str | None:
    if not actor:
        return None
    day = occurred_at.strftime("%Y-%m-%d")
    digest = hashlib.sha256(f"{actor}:{day}".encode()).hexdigest()[:16]
    return digest


def normalize_account1(raw: dict[str, Any], source_account: str = "account1") -> SecurityEvent:
    event = Account1Event.model_validate(raw)
    occurred_at = parse_timestamp(event.eventTime)
    actor = event.userIdentity.get("userName") or event.sourceIPAddress
    outcome = Outcome.FAILURE if event.errorCode else Outcome.SUCCESS
    flags: list[str] = []
    event_type = EventType.API_CALL
    severity = Severity.LOW

    if event.eventName == "ConsoleLogin":
        event_type = EventType.LOGIN
        if outcome == Outcome.FAILURE:
            flags.append("failed_login")
            severity = Severity.MEDIUM
    if event.eventName in {"DeleteBucket", "AssumeRole"}:
        severity = Severity.MEDIUM

    return SecurityEvent(
        source_account=source_account,
        event_type=event_type,
        occurred_at=occurred_at,
        actor=actor,
        target=event.eventName,
        outcome=outcome,
        severity=severity,
        session_id=derive_session_id(actor, occurred_at),
        flags=flags,
        raw_payload=raw,
    )


def normalize_account2(raw: dict[str, Any], source_account: str = "account2") -> SecurityEvent:
    event = Account2Event.model_validate(raw)
    occurred_at = parse_timestamp(event.timestamp)
    outcome = Outcome.SUCCESS if event.status.upper() == "OK" else Outcome.FAILURE
    flags: list[str] = []
    event_type = EventType.LOGIN if event.action == "login" else EventType.UNKNOWN
    severity = Severity.LOW

    if event.action == "login" and outcome == Outcome.FAILURE:
        flags.append("failed_login")
        severity = Severity.MEDIUM
    if event.action == "password_reset":
        severity = Severity.MEDIUM

    return SecurityEvent(
        source_account=source_account,
        event_type=event_type,
        occurred_at=occurred_at,
        actor=event.username,
        target=event.action,
        outcome=outcome,
        severity=severity,
        session_id=derive_session_id(event.username, occurred_at),
        flags=flags,
        raw_payload=raw,
    )


def normalize_account3(raw: dict[str, Any], source_account: str = "account3") -> SecurityEvent:
    event = Account3Event.model_validate(raw)
    occurred_at = parse_timestamp(event.ts)
    flags = list(event.flags)
    outcome = Outcome.FAILURE if "REJECT" in flags else Outcome.SUCCESS
    severity = Severity.LOW

    if "REJECT" in flags:
        severity = Severity.MEDIUM
    if "SUSPICIOUS" in flags:
        flags.append("suspicious_ip")
        severity = Severity.HIGH

    return SecurityEvent(
        source_account=source_account,
        event_type=EventType.NETWORK,
        occurred_at=occurred_at,
        actor=event.src_ip,
        target=event.dst_ip,
        outcome=outcome,
        severity=severity,
        session_id=derive_session_id(event.src_ip, occurred_at),
        flags=flags,
        raw_payload=raw,
    )


NORMALIZERS: dict[str, NormalizerFn] = {
    "account1-events": normalize_account1,
    "account2-events": normalize_account2,
    "account3-events": normalize_account3,
}


def normalize_record(topic: str, raw: dict[str, Any]) -> SecurityEvent:
    normalizer = NORMALIZERS.get(topic)
    if not normalizer:
        raise ValueError(f"No normalizer registered for topic {topic}")
    account = topic.split("-")[0]
    return normalizer(raw, source_account=account)


def validation_error_message(exc: Exception) -> str:
    if isinstance(exc, ValidationError):
        return exc.errors()[0]["msg"]
    return str(exc)
