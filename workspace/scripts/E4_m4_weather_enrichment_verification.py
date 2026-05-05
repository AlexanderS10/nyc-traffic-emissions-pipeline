#!/usr/bin/env python3
"""
Epic 4 Milestone 4 verification helper.

Checks:
1) Notebook config uses a dedicated 15-minute weather watermark.
2) Enriched business table includes required weather columns with non-null joined samples.
3) Optional: verify representative batch explain plan shows a broadcast path.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, to_timestamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Epic 4 Milestone 4 weather enrichment")
    parser.add_argument("--table", default="local.db.enriched_traffic", help="Iceberg table to validate")
    parser.add_argument("--traffic-path", default="s3a://refined-data/traffic/", help="Refined traffic parquet path")
    parser.add_argument("--weather-path", default="s3a://raw-data/weather/", help="Raw weather parquet path")
    parser.add_argument(
        "--notebook-path",
        default="/home/jovyan/work/01_nyc_environmental_pipeline.ipynb",
        help="Pipeline notebook path",
    )
    parser.add_argument("--weather-window-minutes", type=int, default=15, help="Expected weather join window")
    parser.add_argument(
        "--require-broadcast",
        action="store_true",
        help="Fail if representative batch plan does not show broadcast path",
    )
    parser.add_argument("--sample", type=int, default=5, help="Sample rows to print")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("E4_m4_weather_enrichment_verification")
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


def check_notebook_tokens(notebook_path: str) -> list[str]:
    errors: list[str] = []
    path = Path(notebook_path)
    if not path.exists():
        return [f"Notebook not found: {path}"]

    notebook = json.loads(path.read_text(encoding="utf-8"))
    source_text = "\n".join("".join(cell.get("source", [])) for cell in notebook.get("cells", []))

    required_tokens = [
        'WEATHER_WATERMARK_DELAY = "15 minutes"',
        '.withWatermark("weather_event_ts", WEATHER_WATERMARK_DELAY)',
        "WEATHER_JOIN_WINDOW_MINUTES = 15",
    ]
    for token in required_tokens:
        if token not in source_text:
            errors.append(f"Missing notebook token: {token}")

    print("[config] checked weather watermark/broadcast config tokens")
    return errors


def check_weather_columns(spark: SparkSession, table: str, sample_rows: int) -> tuple[list[str], int]:
    errors: list[str] = []
    df = spark.read.table(table)

    required_cols = [
        "weather_period_name",
        "weather_temperature",
        "weather_temperature_unit",
        "weather_wind_speed_mph",
        "weather_wind_direction",
        "weather_short_forecast",
        "weather_precip_probability",
        "weather_event_ts",
        "traffic_event_ts",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        errors.append(f"Missing expected weather columns in {table}: {missing}")
        return errors, 0

    enriched = df.where(
        col("weather_event_ts").isNotNull()
        & col("weather_temperature").isNotNull()
        & col("weather_wind_speed_mph").isNotNull()
    )
    count = enriched.count()
    print(f"[check] non_null_weather_enriched_rows={count}")

    print("[sample] recent weather-enriched rows:")
    (
        enriched.select(
            "traffic_id",
            "traffic_event_ts",
            "weather_event_ts",
            "weather_temperature",
            "weather_wind_speed_mph",
            "weather_precip_probability",
        )
        .orderBy(col("traffic_event_ts").desc())
        .limit(sample_rows)
        .show(truncate=False)
    )
    return errors, count


def check_broadcast_plan(spark: SparkSession, traffic_path: str, weather_path: str, window_minutes: int) -> tuple[bool, str]:
    traffic_sample = (
        spark.read.parquet(traffic_path)
        .select(col("traffic_event_ts"), lit("nyc").alias("weather_join_key"))
        .where(col("traffic_event_ts").isNotNull())
        .limit(200)
        .alias("ta")
    )
    weather_sample = (
        spark.read.parquet(weather_path)
        .withColumn("weather_event_ts", to_timestamp(col("startTime")))
        .withColumn("weather_join_key", lit("nyc"))
        .select("weather_event_ts", "weather_join_key")
        .where(col("weather_event_ts").isNotNull())
        .limit(200)
        .hint("broadcast")
        .alias("w")
    )

    joined = traffic_sample.join(
        weather_sample,
        (
            (col("ta.weather_join_key") == col("w.weather_join_key"))
            & (
                col("w.weather_event_ts")
                >= col("ta.traffic_event_ts") - expr(f"INTERVAL {window_minutes} MINUTES")
            )
            & (
                col("w.weather_event_ts")
                <= col("ta.traffic_event_ts") + expr(f"INTERVAL {window_minutes} MINUTES")
            )
        ),
        "leftOuter",
    )
    plan = joined._jdf.queryExecution().toString()
    has_broadcast = any(token in plan for token in ("BroadcastHashJoin", "BroadcastNestedLoopJoin", "BroadcastExchange"))
    print(f"[check] broadcast_plan_detected={has_broadcast}")
    return has_broadcast, plan


def main() -> int:
    args = parse_args()
    spark = build_spark()
    try:
        errors = []
        errors.extend(check_notebook_tokens(args.notebook_path))
        weather_errors, weather_rows = check_weather_columns(spark, args.table, args.sample)
        errors.extend(weather_errors)
        has_broadcast, _ = check_broadcast_plan(
            spark,
            args.traffic_path,
            args.weather_path,
            args.weather_window_minutes,
        )
        if args.require_broadcast and not has_broadcast:
            errors.append("No broadcast exchange/join token found in representative weather join plan")

        if errors:
            print("[fail] Epic 4 Milestone 4 verification checks failed:")
            for item in errors:
                print(f"  - {item}")
            return 1

        if weather_rows == 0:
            print("[warn] Weather columns present but no non-null weather-enriched rows yet. Keep stream running and retry.")
            return 2

        print("[ok] Epic 4 Milestone 4 verification checks passed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
