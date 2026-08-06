"""Batch job: aggregate raw events into session_current_state."""

from __future__ import annotations

import logging
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

from converge.config import settings
from converge.storage.iceberg import ensure_tables, state_rows_to_arrow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def compute_risk_score(failed_logins: int, flagged: bool, event_count: int) -> float:
    score = min(failed_logins * 2.5, 10.0)
    if flagged:
        score += 5.0
    score += min(event_count * 0.1, 5.0)
    return round(min(score, 100.0), 2)


def build_state_rows(events: list[dict]) -> list[dict]:
    sessions: dict[str, dict] = defaultdict(
        lambda: {
            "failed_login_count": 0,
            "flagged": False,
            "event_count": 0,
            "last_seen": datetime.min.replace(tzinfo=timezone.utc),
            "actor": None,
        }
    )

    for event in events:
        session_id = event.get("session_id")
        if not session_id:
            continue
        state = sessions[session_id]
        state["event_count"] += 1
        state["actor"] = event.get("actor") or state["actor"]
        occurred = event["occurred_at"]
        if isinstance(occurred, str):
            occurred = datetime.fromisoformat(occurred.replace("Z", "+00:00"))
        if occurred > state["last_seen"]:
            state["last_seen"] = occurred
        flags = event.get("flags") or []
        if "failed_login" in flags:
            state["failed_login_count"] += 1
        if "suspicious_ip" in flags or event.get("severity") == "high":
            state["flagged"] = True

    rows: list[dict] = []
    for session_id, state in sessions.items():
        rows.append(
            {
                "session_id": session_id,
                "actor": state["actor"],
                "last_seen": state["last_seen"],
                "failed_login_count": state["failed_login_count"],
                "flagged": state["flagged"],
                "risk_score": compute_risk_score(
                    state["failed_login_count"], state["flagged"], state["event_count"]
                ),
                "event_count": state["event_count"],
            }
        )
    return rows


def run_once() -> int:
    raw_table, state_table = ensure_tables()
    scan = raw_table.scan()
    arrow = scan.to_arrow()
    if arrow.num_rows == 0:
        logger.info("No raw events yet; skipping state rebuild")
        return 0

    events = arrow.to_pylist()
    rows = build_state_rows(events)
    if not rows:
        return 0

    # Full recompute: overwrite derived state table for simplicity/idempotency.
    state_table.overwrite(state_rows_to_arrow(rows))
    logger.info("Rebuilt session_current_state with %d sessions", len(rows))
    return len(rows)


def main() -> None:
    interval = settings.session_job_interval_sec
    logger.info("Session state job running every %ds", interval)
    while True:
        try:
            run_once()
        except Exception:
            logger.exception("Session state job failed")
        time.sleep(interval)


if __name__ == "__main__":
    main()
