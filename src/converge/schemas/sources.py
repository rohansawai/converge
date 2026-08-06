"""Raw per-account input schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class Account1Event(BaseModel):
    """CloudTrail-like event shape."""

    eventTime: str
    eventName: str
    userIdentity: dict[str, Any]
    sourceIPAddress: str
    errorCode: str | None = None


class Account2Event(BaseModel):
    """Auth log event shape."""

    timestamp: str
    username: str
    action: str
    ip: str
    status: str


class Account3Event(BaseModel):
    """VPC flow-like event shape."""

    ts: str
    src_ip: str
    dst_ip: str
    protocol: str
    bytes: int
    flags: list[str] = Field(default_factory=list)
