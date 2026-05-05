#!/usr/bin/env python3
"""
Inject a deliberately late traffic event for watermark verification.

Use this during Milestone 3 manual validation to replay an event older than the
configured watermark window (15 minutes by default).

Example:
  docker exec -it -w /home/jovyan/work jupyter-pyspark \
    python scripts/E3_m3_inject_late_traffic_payload.py --minutes-late 90
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
from datetime import datetime, timedelta

from kafka import KafkaProducer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inject late traffic event for watermark tests")
    parser.add_argument("--bootstrap", default="redpanda:29092", help="Kafka bootstrap server")
    parser.add_argument("--topic", default="nyc_traffic_raw", help="Target Kafka topic")
    parser.add_argument(
        "--minutes-late",
        type=int,
        default=90,
        help="How far in the past to set data_as_of (must exceed watermark delay)",
    )
    parser.add_argument("--record-id", default="late-watermark-probe", help="Traffic record id")
    return parser.parse_args()


def check_socket(host: str, port: int, timeout: float = 3.0) -> None:
    with socket.create_connection((host, port), timeout=timeout):
        print(f"[ok] TCP connectivity to {host}:{port}")


def build_late_payload(record_id: str, minutes_late: int) -> dict[str, str]:
    late_event_ts = datetime.utcnow() - timedelta(minutes=minutes_late)
    # Pipeline expects this timestamp format from NYC traffic records.
    data_as_of = late_event_ts.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    return {
        "id": record_id,
        "status": "0",
        "speed": "25",
        "travel_time": "120",
        "link_name": "Late Watermark Probe",
        "borough": "Manhattan",
        "from_street": "Probe St A",
        "to_street": "Probe St B",
        "data_as_of": data_as_of,
        "link_points": "40.7580,-73.9855",
        "encoded_poly_line": "probe",
    }


def main() -> int:
    args = parse_args()
    host, port_str = args.bootstrap.split(":")
    port = int(port_str)
    check_socket(host, port)

    payload = build_late_payload(args.record_id, args.minutes_late)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: v.encode("utf-8"),
        acks="all",
        retries=3,
    )

    try:
        future = producer.send(args.topic, key=f"late-{args.record_id}", value=payload)
        metadata = future.get(timeout=10)
    finally:
        producer.flush(timeout=10)
        producer.close()

    print(
        "[ok] Late traffic payload injected "
        f"(topic={metadata.topic}, partition={metadata.partition}, offset={metadata.offset})"
    )
    print(f"[info] Injected data_as_of={payload['data_as_of']} (minutes_late={args.minutes_late})")
    print(
        "[info] Next: monitor stream output/table and confirm this late record "
        "is excluded when it is outside watermark delay."
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        raise SystemExit(1)
