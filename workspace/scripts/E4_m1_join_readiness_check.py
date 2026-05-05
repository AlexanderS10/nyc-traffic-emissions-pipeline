#!/usr/bin/env python3
"""
Epic 4 Milestone 1 readiness check.

Validates:
1) Refined traffic/air_quality schemas expose join-critical columns.
2) Notebook externalizes join control constants in one config section.
3) Join-window literals are not hardcoded as raw "INTERVAL 15 MINUTES".
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Epic 4 Milestone 1 readiness checks")
    parser.add_argument("--traffic-path", default="s3a://refined-data/traffic/", help="Refined traffic parquet path")
    parser.add_argument("--aq-path", default="s3a://refined-data/air_quality/", help="Refined AQ parquet path")
    parser.add_argument(
        "--notebook-path",
        default="/home/jovyan/work/01_nyc_environmental_pipeline.ipynb",
        help="Pipeline notebook path",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("E4_m1_join_readiness_check")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,"
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "software.amazon.awssdk:bundle:2.29.38",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def check_refined_schema(spark: SparkSession, traffic_path: str, aq_path: str) -> list[str]:
    errors: list[str] = []

    traffic_required = {"id", "traffic_event_ts", "traffic_lat", "traffic_lon", "h3_index", "speed"}
    aq_required = {"aq_event_ts", "aq_pm25_ugm3", "aq_lat", "aq_lon", "h3_index", "aq_location_id"}

    traffic_cols = set(spark.read.parquet(traffic_path).columns)
    aq_cols = set(spark.read.parquet(aq_path).columns)

    missing_traffic = sorted(traffic_required - traffic_cols)
    missing_aq = sorted(aq_required - aq_cols)

    if missing_traffic:
        errors.append(f"Missing refined traffic columns: {missing_traffic}")
    if missing_aq:
        errors.append(f"Missing refined AQ columns: {missing_aq}")

    print(f"[schema] refined_traffic columns={len(traffic_cols)}")
    print(f"[schema] refined_air_quality columns={len(aq_cols)}")
    return errors


def check_notebook_config(notebook_path: str) -> list[str]:
    errors: list[str] = []
    path = Path(notebook_path)
    if not path.exists():
        return [f"Notebook not found: {path}"]

    notebook = json.loads(path.read_text(encoding="utf-8"))
    source_text = "\n".join("".join(cell.get("source", [])) for cell in notebook.get("cells", []))

    required_tokens = [
        "JOIN_WINDOW_MINUTES",
        "WEATHER_JOIN_WINDOW_MINUTES",
        "STREAM_TRIGGER_INTERVAL",
        "BUSINESS_CHECKPOINT_PATH",
    ]
    for token in required_tokens:
        if token not in source_text:
            errors.append(f"Missing notebook config token: {token}")

    if "INTERVAL 15 MINUTES" in source_text:
        errors.append("Found hardcoded 'INTERVAL 15 MINUTES'; expected constant-based interval expressions")

    print("[config] checked notebook join/trigger/checkpoint constants")
    return errors


def main() -> int:
    args = parse_args()
    spark = build_spark()
    try:
        errors = []
        errors.extend(check_refined_schema(spark, args.traffic_path, args.aq_path))
        errors.extend(check_notebook_config(args.notebook_path))

        if errors:
            print("[fail] Epic 4 Milestone 1 readiness checks failed:")
            for item in errors:
                print(f"  - {item}")
            return 1

        print("[ok] Epic 4 Milestone 1 readiness checks passed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
