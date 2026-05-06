#!/usr/bin/env python3
"""
Epic 4 Milestone 2 verification helper.

Checks joined business output for:
1) Traffic+AQ core metrics in same record (non-null guard result)
2) Temporal window validity (+/- 15 minutes)
3) Sample same-h3 matches for manual inspection
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Epic 4 Milestone 2 joins")
    parser.add_argument("--table", default="local.db.enriched_traffic", help="Iceberg table to validate")
    parser.add_argument("--window-minutes", type=int, default=15, help="Expected traffic/AQ temporal window")
    parser.add_argument("--sample", type=int, default=5, help="Sample rows to print")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("E4_m2_join_verification")
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
        .config("spark.sql.catalog.local.s3.access-key-id", "admin")
        .config("spark.sql.catalog.local.s3.secret-access-key", "password")
        .config("spark.sql.catalog.local.s3.path-style-access", "true")
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
        df = spark.read.table(args.table)

        required_cols = [
            "traffic_id",
            "traffic_event_ts",
            "h3_index",
            "traffic_speed",
            "aq_pm25_ugm3",
            "aq_event_ts",
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"[fail] Missing expected columns in {args.table}: {missing}")
            return 1

        core = df.where(
            col("traffic_speed").isNotNull()
            & col("aq_pm25_ugm3").isNotNull()
            & col("traffic_event_ts").isNotNull()
            & col("aq_event_ts").isNotNull()
            & col("h3_index").isNotNull()
        )
        core_count = core.count()
        print(f"[check] non_null_core_rows={core_count}")

        window_expr = (
            f"aq_event_ts >= traffic_event_ts - INTERVAL {args.window_minutes} MINUTES "
            f"AND aq_event_ts <= traffic_event_ts + INTERVAL {args.window_minutes} MINUTES"
        )
        in_window = core.where(expr(window_expr))
        in_window_count = in_window.count()
        print(f"[check] in_window_rows={in_window_count}")

        sample_df = (
            in_window.select(
                "traffic_id",
                "h3_index",
                "traffic_event_ts",
                "aq_event_ts",
                "traffic_speed",
                "aq_pm25_ugm3",
            )
            .orderBy(col("traffic_event_ts").desc())
            .limit(args.sample)
        )
        print("[sample] recent same-h3 in-window rows:")
        sample_df.show(truncate=False)

        if in_window_count == 0:
            print("[warn] No validated in-window rows found yet. Keep stream running and retry.")
            return 2

        print("[ok] Epic 4 Milestone 2 verification checks passed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
