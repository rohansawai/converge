"""Streamlit CISO-style security dashboard."""

from __future__ import annotations

import json
import time

import streamlit as st
from kafka import KafkaConsumer

from converge.config import settings
from converge.queries.metrics import (
    account_leaderboard,
    failed_logins_24h,
    top_flagged_ips,
    total_events_24h,
)


def get_dlq_count() -> int:
    try:
        consumer = KafkaConsumer(
            settings.topic_dlq,
            bootstrap_servers=settings.kafka_bootstrap.split(","),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=2000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        count = sum(1 for _ in consumer)
        consumer.close()
        return count
    except Exception:
        return 0


def main() -> None:
    st.set_page_config(page_title="Converge Security Dashboard", layout="wide")
    st.title("Converge Security Lakehouse")
    st.caption("Cross-account security metrics from federated event streams")

    refresh = st.sidebar.slider("Auto-refresh (seconds)", min_value=5, max_value=60, value=15)
    if st.sidebar.button("Refresh now"):
        st.rerun()

    col1, col2, col3, col4 = st.columns(4)
    try:
        total = total_events_24h()
        failed = failed_logins_24h()
        dlq = get_dlq_count()
    except Exception as exc:
        st.error(f"Query error: {exc}")
        total = failed = dlq = 0

    col1.metric("Events (24h)", total)
    col2.metric("Failed logins (24h)", failed)
    col3.metric("DLQ records", dlq)
    col4.metric("Pipeline", "Healthy" if dlq < 100 else "Degraded")

    st.subheader("Top flagged actors")
    flagged = top_flagged_ips()
    if flagged:
        st.dataframe(flagged, use_container_width=True)
    else:
        st.info("No flagged sessions yet. Events are accumulating.")

    st.subheader("Account event leaderboard")
    leaderboard = account_leaderboard()
    if leaderboard:
        st.bar_chart({row["source_account"]: row["event_count"] for row in leaderboard})
        st.dataframe(leaderboard, use_container_width=True)
    else:
        st.info("No events in Iceberg yet.")

    with st.expander("Failure demo"):
        st.markdown(
            """
            Simulate a source outage:
            ```bash
            docker compose stop generator-account2
            ```
            Restart with:
            ```bash
            docker compose start generator-account2
            ```
            """
        )

    time.sleep(refresh)
    st.rerun()


if __name__ == "__main__":
    main()
