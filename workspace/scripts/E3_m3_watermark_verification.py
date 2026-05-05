#!/usr/bin/env python3
"""
Milestone 3 verification helper: late-event exclusion + watermark snapshot.

Runs three checks:
1) Raw present: late probe record exists in raw traffic sink.
2) Enriched absent: late probe record is excluded from business table.
3) Watermark snapshot: prints event-time ranges and freshness proxy.

Run in container:
  docker exec -it -w /home/jovyan/work jupyter-pyspark \
    python scripts/E3_m3_watermark_verification.py --record-id late-probe-001
"""

from __future__ import annotations

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, max as spark_max, min as spark_min


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Milestone 3 watermark verification")
    parser.add_argument("--record-id", required=True, help="Injected late record id")
    parser.add_argument("--raw-traffic-path", default="s3a://raw-data/traffic/", help="Raw traffic path")
    parser.add_argument(
        "--enriched-table",
        default="local.db.enriched_traffic",
        help="Business enriched table name",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("E3_m3_watermark_verification")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,"
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "software.amazon.awssdk:bundle:2.29.38",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "rest")
        .config("spark.sql.catalog.local.uri", "http://rest-catalog:8181")
        .config("spark.sql.catalog.local.warehouse", "s3a://business-data/")
        .config("spark.sql.catalog.local.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.local.s3.path-style-access", "true")
        .config("spark.sql.catalog.local.s3.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def main() -> int:
    args = parse_args()
    spark = build_spark()

    try:
        print(f"[info] Verifying late record id={args.record_id}")

        raw_df = spark.read.parquet(args.raw_traffic_path)
        raw_hits = raw_df.where(col("id") == args.record_id)
        raw_count = raw_hits.count()
        print(f"[check] Raw present count={raw_count}")
        raw_hits.select("id", "data_as_of", "kafka_timestamp").orderBy(col("kafka_timestamp").desc()).show(
            truncate=False
        )

        enriched_df = spark.table(args.enriched_table)
        enriched_hits = enriched_df.where(col("traffic_id") == args.record_id)
        enriched_count = enriched_hits.count()
        print(f"[check] Enriched present count={enriched_count} (expected 0)")
        enriched_hits.select("traffic_id", "traffic_event_ts", "aq_event_ts", "weather_event_ts").show(
            truncate=False
        )

        watermark_snapshot = (
            enriched_df.select(
                spark_min("traffic_event_ts").alias("min_traffic_event_ts"),
                spark_max("traffic_event_ts").alias("max_traffic_event_ts"),
                spark_min("aq_event_ts").alias("min_aq_event_ts"),
                spark_max("aq_event_ts").alias("max_aq_event_ts"),
                spark_min("weather_event_ts").alias("min_weather_event_ts"),
                spark_max("weather_event_ts").alias("max_weather_event_ts"),
            )
            .withColumn("now_ts", current_timestamp())
            .withColumn(
                "traffic_max_lag_minutes",
                expr("round((unix_timestamp(now_ts) - unix_timestamp(max_traffic_event_ts)) / 60.0, 2)"),
            )
        )
        print("[check] Watermark/freshness snapshot proxy:")
        watermark_snapshot.show(truncate=False)

        if raw_count == 0:
            print("[error] Injected late record not found in raw sink. Verify injection and stream health.")
            return 2
        if enriched_count > 0:
            print(
                "[error] Late record is present in enriched output; expected exclusion for out-of-watermark event."
            )
            return 3

        print("[ok] Verification passed: raw record present, enriched record absent, snapshot printed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        raise SystemExit(1)
