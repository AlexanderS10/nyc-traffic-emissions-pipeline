#!/usr/bin/env python3
"""
Milestone 1 smoke test: validate Kafka connectivity and required topics.

Run inside the Jupyter container for in-network DNS resolution:
  docker exec -it jupyter-pyspark python /home/jovyan/work/scripts/E1_m1_stream_source_smoke.py
"""

import argparse
import socket
import sys

from kafka import KafkaAdminClient


DEFAULT_TOPICS = [
    "nyc_traffic_raw",
    "nyc_openaq_raw",
    "nyc_purpleair_raw",
    "nyc_weather_raw",
]


def check_socket(host: str, port: int, timeout: float = 3.0) -> None:
    with socket.create_connection((host, port), timeout=timeout):
        print(f"[ok] TCP connectivity to {host}:{port}")


def check_topics(bootstrap: str, required_topics: list[str]) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="m1-smoke")
    try:
        topic_names = set(admin.list_topics())
    finally:
        admin.close()

    print(f"[ok] Kafka admin reachable at {bootstrap}")
    print(f"[info] Topics discovered: {len(topic_names)}")

    missing = [topic for topic in required_topics if topic not in topic_names]
    if missing:
        raise RuntimeError(f"Missing required topics: {missing}")

    print(f"[ok] Required topics are present: {required_topics}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Milestone 1 stream source smoke test")
    parser.add_argument(
        "--bootstrap",
        default="redpanda:29092",
        help="Kafka bootstrap server, default: redpanda:29092",
    )
    parser.add_argument(
        "--topics",
        nargs="*",
        default=DEFAULT_TOPICS,
        help="Required topics to validate",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    host, port_str = args.bootstrap.split(":")
    port = int(port_str)

    print(f"[info] Checking bootstrap endpoint: {args.bootstrap}")
    check_socket(host, port)
    check_topics(args.bootstrap, args.topics)
    print("[ok] Milestone 1 source connectivity smoke test passed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        raise SystemExit(1)
