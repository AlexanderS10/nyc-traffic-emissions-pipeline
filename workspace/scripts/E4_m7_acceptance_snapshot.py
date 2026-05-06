#!/usr/bin/env python3
"""
Epic 4 Milestone 7 acceptance snapshot.

Captures reproducible evidence for reviewer handoff:
- total row count
- coverage for AQ/weather/static flags
- borough-level counts and match percentages
- latest event-time window
- lightweight query timing snapshot
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Capture Epic 4 acceptance snapshot")
    parser.add_argument("--table", default="local.db.enriched_traffic", help="Iceberg table name")
    parser.add_argument("--sample", type=int, default=10, help="Sample rows to print")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("E4_m7_acceptance_snapshot")
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


def timed(action_name: str, fn):
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    print(f"[timing] {action_name}_elapsed_s={elapsed:.3f}")
    return result, elapsed


def main() -> int:
    args = parse_args()
    spark = build_spark()
    now = datetime.now(timezone.utc).isoformat()
    print(f"[snapshot] generated_at_utc={now}")
    print(f"[snapshot] table={args.table}")

    try:
        df = spark.read.table(args.table)

        required_cols = [
            "traffic_id",
            "traffic_borough",
            "traffic_event_ts",
            "aq_pm25_ugm3",
            "weather_temperature",
            "is_congestion_zone",
            "is_truck_route",
            "has_aq_match",
            "has_weather_match",
        ]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"[fail] Missing required columns: {missing}")
            return 1

        (total_rows, _), _ = timed("total_rows_count", lambda: (df.count(), None))
        print(f"[snapshot] total_rows={total_rows}")

        if total_rows == 0:
            print("[warn] Table is empty; keep stream running and rerun snapshot.")
            return 2

        coverage = (
            df.selectExpr(
                "sum(case when has_aq_match then 1 else 0 end) as aq_match_rows",
                "sum(case when has_weather_match then 1 else 0 end) as weather_match_rows",
                "sum(case when is_congestion_zone then 1 else 0 end) as congestion_rows",
                "sum(case when is_truck_route then 1 else 0 end) as truck_route_rows",
                "min(traffic_event_ts) as min_traffic_event_ts",
                "max(traffic_event_ts) as max_traffic_event_ts",
            )
            .collect()[0]
        )
        print(
            "[snapshot] coverage "
            f"aq_match_rows={coverage['aq_match_rows']} "
            f"weather_match_rows={coverage['weather_match_rows']} "
            f"congestion_rows={coverage['congestion_rows']} "
            f"truck_route_rows={coverage['truck_route_rows']}"
        )
        print(
            "[snapshot] event_window "
            f"min_traffic_event_ts={coverage['min_traffic_event_ts']} "
            f"max_traffic_event_ts={coverage['max_traffic_event_ts']}"
        )

        print("[snapshot] borough distribution and match percentages:")
        (
            df.groupBy("traffic_borough")
            .agg(
                expr("count(*) as total_rows"),
                expr("sum(case when has_aq_match then 1 else 0 end) as aq_rows"),
                expr("sum(case when has_weather_match then 1 else 0 end) as weather_rows"),
                expr("sum(case when is_congestion_zone then 1 else 0 end) as congestion_rows"),
                expr("sum(case when is_truck_route then 1 else 0 end) as truck_rows"),
            )
            .selectExpr(
                "traffic_borough",
                "total_rows",
                "aq_rows",
                "weather_rows",
                "congestion_rows",
                "truck_rows",
                "round(100.0 * aq_rows / total_rows, 2) as aq_match_pct",
                "round(100.0 * weather_rows / total_rows, 2) as weather_match_pct",
            )
            .orderBy(col("total_rows").desc())
            .show(truncate=False)
        )

        print("[snapshot] recent sample rows:")
        (
            df.select(
                "traffic_id",
                "traffic_borough",
                "h3_index",
                "aq_pm25_ugm3",
                "weather_temperature",
                "is_congestion_zone",
                "is_truck_route",
                "has_aq_match",
                "has_weather_match",
                "traffic_event_ts",
            )
            .orderBy(col("traffic_event_ts").desc())
            .limit(args.sample)
            .show(truncate=False)
        )

        print("[ok] Epic 4 Milestone 7 acceptance snapshot completed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
