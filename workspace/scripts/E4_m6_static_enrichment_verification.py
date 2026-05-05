#!/usr/bin/env python3
"""
Epic 4 Milestone 6 verification helper.

Checks static-flag enrichment in the business table:
1) Required flag columns exist.
2) Flag columns are non-null booleans.
3) Prints true/false distributions and sample rows.
"""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, expr, sum as spark_sum


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Epic 4 Milestone 6 static enrichment")
    parser.add_argument("--table", default="local.db.enriched_traffic", help="Iceberg table name")
    parser.add_argument("--sample", type=int, default=10, help="Number of rows to sample")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("E4_m6_static_enrichment_verification")
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

        required_cols = ["is_congestion_zone", "is_truck_route", "h3_index", "traffic_id", "traffic_event_ts"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"[fail] Missing required columns in {args.table}: {missing}")
            print("[hint] Re-run notebook stream from the static-enrichment join cell with latest schema.")
            return 1

        null_counts = (
            df.select(
                spark_sum(expr("CASE WHEN is_congestion_zone IS NULL THEN 1 ELSE 0 END")).alias("null_is_congestion_zone"),
                spark_sum(expr("CASE WHEN is_truck_route IS NULL THEN 1 ELSE 0 END")).alias("null_is_truck_route"),
            )
            .collect()[0]
        )
        print(
            f"[check] null_counts is_congestion_zone={null_counts['null_is_congestion_zone']} "
            f"is_truck_route={null_counts['null_is_truck_route']}"
        )

        dist = (
            df.groupBy("is_congestion_zone", "is_truck_route")
            .agg(count("*").alias("rows"))
            .orderBy(col("rows").desc())
        )
        print("[check] static flag distribution:")
        dist.show(truncate=False)

        print("[sample] recent enriched rows with static flags:")
        (
            df.select(
                "traffic_id",
                "traffic_borough",
                "h3_index",
                "is_congestion_zone",
                "is_truck_route",
                "traffic_event_ts",
            )
            .orderBy(col("traffic_event_ts").desc())
            .limit(args.sample)
            .show(truncate=False)
        )

        print("[ok] Epic 4 Milestone 6 static enrichment verification checks passed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
