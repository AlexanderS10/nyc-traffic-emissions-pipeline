#!/usr/bin/env python3
"""
Inject one malformed JSON payload into a Kafka topic.

Use this to validate Milestone 2 quarantine behavior.

Example:
  docker exec -it jupyter-pyspark python /home/jovyan/work/scripts/E3_m2_inject_malformed_payload.py
"""

from __future__ import annotations

import argparse
import socket
import sys
import time

from kafka import KafkaProducer


DEFAULT_BOOTSTRAP = "redpanda:29092"
DEFAULT_TOPIC = "nyc_purpleair_raw"
DEFAULT_PAYLOAD = '{"bad_json": true'  # Intentionally malformed (missing closing brace)


def check_socket(host: str, port: int, timeout: float = 3.0) -> None:
    with socket.create_connection((host, port), timeout=timeout):
        print(f"[ok] TCP connectivity to {host}:{port}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inject malformed JSON into Kafka topic")
    parser.add_argument("--bootstrap", default=DEFAULT_BOOTSTRAP, help="Kafka bootstrap server")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Target Kafka topic")
    parser.add_argument(
        "--payload",
        default=DEFAULT_PAYLOAD,
        help="Malformed payload to inject",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    host, port_str = args.bootstrap.split(":")
    port = int(port_str)

    print(f"[info] Target topic: {args.topic}")
    print(f"[info] Bootstrap: {args.bootstrap}")
    check_socket(host, port)

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        acks="all",
        retries=3,
        linger_ms=0,
        value_serializer=lambda v: v.encode("utf-8"),
    )

    try:
        future = producer.send(args.topic, value=args.payload)
        metadata = future.get(timeout=10)
        print(
            "[ok] Malformed payload injected "
            f"(topic={metadata.topic}, partition={metadata.partition}, offset={metadata.offset})"
        )
    finally:
        producer.flush(timeout=10)
        producer.close()

    print("[info] Next step: verify this record appears in malformed JSON quarantine output.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        time.sleep(0.05)
        raise SystemExit(1)
