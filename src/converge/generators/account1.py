"""Account1 generator: CloudTrail-like events."""

from __future__ import annotations

import os
import random
from typing import Any

from converge.config import settings
from converge.generators.base import BaseGenerator, build_arg_parser, utc_now_iso

USERS = ["alice", "bob", "carol", "dave", "eve"]
EVENTS = ["ConsoleLogin", "GetObject", "PutObject", "DeleteBucket", "AssumeRole"]
IPS = ["203.0.113.1", "198.51.100.42", "192.0.2.55", "10.0.0.15", "172.16.0.8"]


class Account1Generator(BaseGenerator):
    def generate_valid(self) -> dict[str, Any]:
        event_name = random.choice(EVENTS)
        error = None
        if event_name == "ConsoleLogin" and random.random() < 0.15:
            error = "Failed authentication"
        return {
            "eventTime": utc_now_iso(),
            "eventName": event_name,
            "userIdentity": {"userName": random.choice(USERS)},
            "sourceIPAddress": random.choice(IPS),
            "errorCode": error,
        }

    def generate_malformed(self) -> dict[str, Any]:
        choice = random.randint(0, 2)
        if choice == 0:
            return {"eventTime": "not-a-timestamp", "eventName": "ConsoleLogin"}
        if choice == 1:
            return {"eventName": "ConsoleLogin", "sourceIPAddress": "203.0.113.1"}
        return {"eventTime": utc_now_iso(), "userIdentity": {}, "sourceIPAddress": "bad-ip"}


def main() -> None:
    parser = build_arg_parser("Account1 CloudTrail-like generator")
    args = parser.parse_args()
    stdout_only = args.stdout_only or not (
        args.kafka_bootstrap or os.environ.get("KAFKA_BOOTSTRAP")
    )
    generator = Account1Generator(
        rate=args.rate,
        malformed_rate=args.malformed_rate,
        stdout_only=stdout_only,
        kafka_bootstrap=args.kafka_bootstrap or os.environ.get("KAFKA_BOOTSTRAP"),
        topic=args.topic or os.environ.get("KAFKA_TOPIC") or settings.topic_account1,
        max_events=args.max_events,
    )
    generator.run()


if __name__ == "__main__":
    main()
