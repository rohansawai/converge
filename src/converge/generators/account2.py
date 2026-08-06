"""Account2 generator: auth-log events."""

from __future__ import annotations

import os
import random
from typing import Any

from converge.config import settings
from converge.generators.base import BaseGenerator, build_arg_parser, utc_now_iso

USERS = ["bob", "frank", "grace", "heidi", "ivan"]
ACTIONS = ["login", "logout", "password_reset", "mfa_challenge"]
IPS = ["198.51.100.42", "203.0.113.77", "192.0.2.10", "10.1.2.3"]


class Account2Generator(BaseGenerator):
    def generate_valid(self) -> dict[str, Any]:
        action = random.choice(ACTIONS)
        status = "FAIL" if action == "login" and random.random() < 0.2 else "OK"
        return {
            "timestamp": utc_now_iso(),
            "username": random.choice(USERS),
            "action": action,
            "ip": random.choice(IPS),
            "status": status,
        }

    def generate_malformed(self) -> dict[str, Any]:
        choice = random.randint(0, 2)
        if choice == 0:
            return {"timestamp": utc_now_iso(), "action": "login"}
        if choice == 1:
            return {"username": "bob", "ip": "198.51.100.42", "status": "OK"}
        return {"timestamp": "yesterday", "username": "", "action": "login", "ip": "x", "status": "MAYBE"}


def main() -> None:
    parser = build_arg_parser("Account2 auth-log generator")
    args = parser.parse_args()
    stdout_only = args.stdout_only or not (
        args.kafka_bootstrap or os.environ.get("KAFKA_BOOTSTRAP")
    )
    generator = Account2Generator(
        rate=args.rate,
        malformed_rate=args.malformed_rate,
        stdout_only=stdout_only,
        kafka_bootstrap=args.kafka_bootstrap or os.environ.get("KAFKA_BOOTSTRAP"),
        topic=args.topic or os.environ.get("KAFKA_TOPIC") or settings.topic_account2,
        max_events=args.max_events,
    )
    generator.run()


if __name__ == "__main__":
    main()
