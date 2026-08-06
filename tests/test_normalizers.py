"""Normalizer unit tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from converge.normalizers import (
    normalize_account1,
    normalize_account2,
    normalize_account3,
    normalize_record,
    parse_timestamp,
)
from converge.schemas.canonical import EventType, Outcome, Severity

FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def test_parse_timestamp_z_suffix() -> None:
    dt = parse_timestamp("2026-01-15T10:00:00Z")
    assert dt.year == 2026
    assert dt.hour == 10


def test_parse_timestamp_invalid_raises() -> None:
    with pytest.raises(ValueError):
        parse_timestamp("not-a-timestamp")


def test_normalize_account1_happy_path() -> None:
    raw = load_fixture("account1_valid.json")
    event = normalize_account1(raw)
    assert event.source_account == "account1"
    assert event.event_type == EventType.LOGIN
    assert event.outcome == Outcome.FAILURE
    assert "failed_login" in event.flags
    assert event.severity == Severity.MEDIUM
    assert event.actor == "alice"


def test_normalize_account1_api_call_success() -> None:
    raw = {
        "eventTime": "2026-01-15T10:00:00Z",
        "eventName": "GetObject",
        "userIdentity": {"userName": "alice"},
        "sourceIPAddress": "203.0.113.1",
        "errorCode": None,
    }
    event = normalize_account1(raw)
    assert event.event_type == EventType.API_CALL
    assert event.outcome == Outcome.SUCCESS


def test_normalize_account1_missing_fields_raises() -> None:
    with pytest.raises(ValidationError):
        normalize_account1({"eventTime": "2026-01-15T10:00:00Z"})


def test_normalize_account2_happy_path() -> None:
    raw = load_fixture("account2_valid.json")
    event = normalize_account2(raw)
    assert event.source_account == "account2"
    assert event.event_type == EventType.LOGIN
    assert event.outcome == Outcome.FAILURE
    assert "failed_login" in event.flags


def test_normalize_account2_success_login() -> None:
    raw = {
        "timestamp": "2026-01-15T10:00:00Z",
        "username": "bob",
        "action": "login",
        "ip": "198.51.100.42",
        "status": "OK",
    }
    event = normalize_account2(raw)
    assert event.outcome == Outcome.SUCCESS
    assert event.flags == []


def test_normalize_account2_missing_fields_raises() -> None:
    with pytest.raises(ValidationError):
        normalize_account2({"timestamp": "2026-01-15T10:00:00Z", "action": "login"})


def test_normalize_account3_happy_path() -> None:
    raw = load_fixture("account3_valid.json")
    event = normalize_account3(raw)
    assert event.event_type == EventType.NETWORK
    assert event.outcome == Outcome.FAILURE
    assert "suspicious_ip" in event.flags
    assert event.severity == Severity.HIGH


def test_normalize_account3_accepted_flow() -> None:
    raw = {
        "ts": "2026-01-15T10:00:00Z",
        "src_ip": "10.0.1.5",
        "dst_ip": "203.0.113.99",
        "protocol": "TCP",
        "bytes": 512,
        "flags": [],
    }
    event = normalize_account3(raw)
    assert event.outcome == Outcome.SUCCESS
    assert event.severity == Severity.LOW


def test_normalize_account3_missing_fields_raises() -> None:
    with pytest.raises(ValidationError):
        normalize_account3({"ts": "2026-01-15T10:00:00Z", "src_ip": "10.0.1.5"})


def test_normalize_record_routes_by_topic() -> None:
    raw = load_fixture("account1_valid.json")
    event = normalize_record("account1-events", raw)
    assert event.source_account == "account1"


def test_normalize_record_unknown_topic() -> None:
    with pytest.raises(ValueError):
        normalize_record("unknown-topic", {})


def test_session_id_derived_for_actor() -> None:
    raw = load_fixture("account2_valid.json")
    event = normalize_account2(raw)
    assert event.session_id is not None
    assert len(event.session_id) == 16
