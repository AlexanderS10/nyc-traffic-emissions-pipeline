#!/usr/bin/env python3
"""
Milestone 1 full integration check.

Checks:
1) Kafka bootstrap TCP reachability and required topic presence.
2) Spark Master UI endpoint reachability.
3) Structured Streaming query activity for traffic + openaq topics.

Recommended run location (inside compose network):
  docker exec -it jupyter-pyspark python /home/jovyan/work/scripts/E1_m1_full_integration_check.py
"""

from __future__ import annotations

import argparse
import json
import socket
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from uuid import uuid4

from kafka import KafkaAdminClient
from pyspark.sql import SparkSession


DEFAULT_TOPICS = ["nyc_traffic_raw", "nyc_openaq_raw"]
KAFKA_SQL_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2"


def check_socket(host: str, port: int, timeout: float = 3.0) -> None:
    with socket.create_connection((host, port), timeout=timeout):
        print(f"[ok] TCP connectivity to {host}:{port}")


def check_topics(bootstrap: str, required_topics: list[str]) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, client_id="m1-full-check")
    try:
        topic_names = set(admin.list_topics())
    finally:
        admin.close()

    missing = [topic for topic in required_topics if topic not in topic_names]
    if missing:
        raise RuntimeError(f"Missing required topics: {missing}")

    print(f"[ok] Kafka admin reachable at {bootstrap}")
    print(f"[ok] Required topics are present: {required_topics}")


def check_http_json(url: str, timeout: float = 5.0) -> dict:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            if response.status != 200:
                raise RuntimeError(f"Unexpected status {response.status} for {url}")
            payload = response.read().decode("utf-8")
            data = json.loads(payload)
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Failed to reach {url}: {exc}") from exc

    print(f"[ok] HTTP endpoint reachable: {url}")
    return data


def build_spark(master_url: str, bootstrap: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("m1_full_integration_check")
        .master(master_url)
        .config("spark.jars.packages", KAFKA_SQL_PKG)
        .config("spark.driver.host", "jupyter-pyspark")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.kafka.bootstrap.servers", bootstrap)
        .getOrCreate()
    )
    print(f"[ok] Spark session created: version={spark.version}, master={master_url}")
    return spark


def wait_for_progress(queries: list, timeout_s: int) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        active = [q.isActive for q in queries]
        if not all(active):
            raise RuntimeError("One or more streaming queries became inactive unexpectedly.")

        if any(q.lastProgress is not None for q in queries):
            print("[ok] At least one streaming query reported progress.")
            return

        time.sleep(1.0)

    print("[warn] No micro-batch progress yet, but queries are still active.")


def run_stream_activity_check(
    spark: SparkSession,
    bootstrap: str,
    traffic_topic: str,
    openaq_topic: str,
    starting_offsets: str,
    wait_seconds: int,
) -> None:
    run_id = uuid4().hex[:10]
    base_ckpt = Path(f"/tmp/m1_full_check_ckpt_{run_id}")
    traffic_ckpt = str(base_ckpt / "traffic")
    openaq_ckpt = str(base_ckpt / "openaq")
    print(f"[info] Using temporary checkpoints under: {base_ckpt}")

    traffic_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", traffic_topic)
        .option("startingOffsets", starting_offsets)
        .load()
        .selectExpr("CAST(value AS STRING) AS payload")
    )

    openaq_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", openaq_topic)
        .option("startingOffsets", starting_offsets)
        .load()
        .selectExpr("CAST(value AS STRING) AS payload")
    )

    traffic_query = (
        traffic_stream.writeStream
        .format("memory")
        .queryName(f"m1_traffic_probe_{run_id}")
        .outputMode("append")
        .option("checkpointLocation", traffic_ckpt)
        .start()
    )

    openaq_query = (
        openaq_stream.writeStream
        .format("memory")
        .queryName(f"m1_openaq_probe_{run_id}")
        .outputMode("append")
        .option("checkpointLocation", openaq_ckpt)
        .start()
    )

    print("[ok] Started two structured streaming probe queries.")
    try:
        wait_for_progress([traffic_query, openaq_query], timeout_s=wait_seconds)
        if traffic_query.isActive and openaq_query.isActive:
            print("[ok] Both probe streaming queries are active.")
        else:
            raise RuntimeError("Probe queries are not both active.")
    finally:
        for query in (traffic_query, openaq_query):
            if query.isActive:
                query.stop()
        print("[ok] Probe streaming queries stopped cleanly.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Milestone 1 full integration check")
    parser.add_argument("--bootstrap", default="redpanda:29092")
    parser.add_argument("--spark-master", default="spark://spark-master:7077")
    parser.add_argument("--spark-ui-json", default="http://spark-master:8080/json/")
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--wait-seconds", type=int, default=20)
    parser.add_argument(
        "--topics",
        nargs="*",
        default=DEFAULT_TOPICS,
        help="First topic is traffic, second is openaq",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if len(args.topics) < 2:
        raise ValueError("--topics requires at least two topics: traffic and openaq")

    host, port_str = args.bootstrap.split(":")
    port = int(port_str)

    print("[info] Step 1/3: Kafka connectivity and topic presence")
    check_socket(host, port)
    check_topics(args.bootstrap, args.topics[:2])

    print("[info] Step 2/3: Spark master endpoint check")
    spark_state = check_http_json(args.spark_ui_json)
    worker_count = len(spark_state.get("workers", [])) if isinstance(spark_state, dict) else 0
    print(f"[info] Spark master reports workers: {worker_count}")

    print("[info] Step 3/3: Structured streaming activity check")
    spark = build_spark(args.spark_master, args.bootstrap)
    try:
        run_stream_activity_check(
            spark=spark,
            bootstrap=args.bootstrap,
            traffic_topic=args.topics[0],
            openaq_topic=args.topics[1],
            starting_offsets=args.starting_offsets,
            wait_seconds=args.wait_seconds,
        )
    finally:
        spark.stop()
        print("[ok] Spark session stopped cleanly.")

    print("[ok] Milestone 1 full integration check passed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        raise SystemExit(1)
