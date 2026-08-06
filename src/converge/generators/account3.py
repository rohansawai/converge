"""Account3 generator: VPC flow-like events."""

from __future__ import annotations

import os
import random
from typing import Any

from converge.config import settings
from converge.generators.base import BaseGenerator, build_arg_parser, utc_now_iso

SRC_IPS = ["10.0.1.5", "10.0.2.12", "10.0.3.8", "172.16.5.1"]
DST_IPS = ["203.0.113.99", "198.51.100.1", "8.8.8.8", "10.0.1.99"]
PROTOCOLS = ["TCP", "UDP", "ICMP"]


class Account3Generator(BaseGenerator):
    def generate_valid(self) -> dict[str, Any]:
        flags: list[str] = []
        if random.random() < 0.12:
            flags.append("REJECT")
        if random.random() < 0.05:
            flags.append("SUSPICIOUS")
        return {
            "ts": utc_now_iso(),
            "src_ip": random.choice(SRC_IPS),
            "dst_ip": random.choice(DST_IPS),
            "protocol": random.choice(PROTOCOLS),
            "bytes": random.randint(64, 65536),
            "flags": flags,
        }

    def generate_malformed(self) -> dict[str, Any]:
        choice = random.randint(0, 2)
        if choice == 0:
            return {"ts": utc_now_iso(), "src_ip": "10.0.1.5"}
        if choice == 1:
            return {"dst_ip": "203.0.113.99", "protocol": "TCP", "bytes": "lots"}
        return {"ts": "invalid", "src_ip": "", "dst_ip": "", "protocol": "TCP", "bytes": -1}


def main() -> None:
    parser = build_arg_parser("Account3 VPC flow-like generator")
    args = parser.parse_args()
    stdout_only = args.stdout_only or not (
        args.kafka_bootstrap or os.environ.get("KAFKA_BOOTSTRAP")
    )
    generator = Account3Generator(
        rate=args.rate,
        malformed_rate=args.malformed_rate,
        stdout_only=stdout_only,
        kafka_bootstrap=args.kafka_bootstrap or os.environ.get("KAFKA_BOOTSTRAP"),
        topic=args.topic or os.environ.get("KAFKA_TOPIC") or settings.topic_account3,
        max_events=args.max_events,
    )
    generator.run()


if __name__ == "__main__":
    main()
