"""DuckDB queries over Iceberg tables for dashboard metrics."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import duckdb

from converge.storage.iceberg import ensure_tables


def _connect() -> duckdb.DuckDBPyConnection:
    return duckdb.connect(database=":memory:")


def _load_raw_events(con: duckdb.DuckDBPyConnection) -> bool:
    raw_table, _ = ensure_tables()
    arrow = raw_table.scan().to_arrow()
    if arrow.num_rows == 0:
        return False
    con.register("raw_events", arrow)
    return True


def _load_state(con: duckdb.DuckDBPyConnection) -> bool:
    _, state_table = ensure_tables()
    arrow = state_table.scan().to_arrow()
    if arrow.num_rows == 0:
        return False
    con.register("session_state", arrow)
    return True


def total_events_24h() -> int:
    con = _connect()
    try:
        if not _load_raw_events(con):
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        result = con.execute(
            "SELECT COUNT(*) FROM raw_events WHERE occurred_at >= ?",
            [cutoff],
        ).fetchone()
        return int(result[0]) if result else 0
    except Exception:
        return 0
    finally:
        con.close()


def failed_logins_24h() -> int:
    con = _connect()
    try:
        if not _load_raw_events(con):
            return 0
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        result = con.execute(
            """
            SELECT COUNT(*) FROM raw_events
            WHERE occurred_at >= ?
              AND list_contains(COALESCE(flags, []), 'failed_login')
            """,
            [cutoff],
        ).fetchone()
        return int(result[0]) if result else 0
    except Exception:
        return 0
    finally:
        con.close()


def top_flagged_ips(limit: int = 10) -> list[dict[str, Any]]:
    con = _connect()
    try:
        if not _load_state(con):
            return []
        rows = con.execute(
            """
            SELECT actor, risk_score, failed_login_count, event_count
            FROM session_state
            WHERE flagged = true
            ORDER BY risk_score DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [
            {
                "actor": r[0],
                "risk_score": r[1],
                "failed_login_count": r[2],
                "event_count": r[3],
            }
            for r in rows
        ]
    except Exception:
        return []
    finally:
        con.close()


def account_leaderboard() -> list[dict[str, Any]]:
    con = _connect()
    try:
        if not _load_raw_events(con):
            return []
        rows = con.execute(
            """
            SELECT source_account, COUNT(*) AS event_count
            FROM raw_events
            GROUP BY source_account
            ORDER BY event_count DESC
            """
        ).fetchall()
        return [{"source_account": r[0], "event_count": r[1]} for r in rows]
    except Exception:
        return []
    finally:
        con.close()
