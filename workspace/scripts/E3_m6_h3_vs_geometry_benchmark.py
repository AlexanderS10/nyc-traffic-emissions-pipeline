#!/usr/bin/env python3
"""
Milestone 6 benchmark: H3 equality join vs geometric-distance-style join.

This script compares two join strategies over sampled refined datasets:
1) H3 strategy: equality join on h3_index (+ event-time window)
2) Geometry-style baseline: distance threshold using haversine (+ event-time window)

Output:
- elapsed time (seconds) for each strategy
- output row count for each strategy
- relative speedup ratio
- a markdown report template with placeholders for Spark UI metrics

Run in container:
  docker exec -it -w /home/jovyan/work jupyter-pyspark \
    python scripts/E3_m6_h3_vs_geometry_benchmark.py
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import col, expr, floor, unix_timestamp


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark H3 join vs geometric baseline")
    parser.add_argument("--traffic-path", default="s3a://refined-data/traffic/", help="Refined traffic path")
    parser.add_argument("--aq-path", default="s3a://refined-data/air_quality/", help="Refined AQ path")
    parser.add_argument("--hours-back", type=int, default=6, help="Time horizon to sample from recent data")
    parser.add_argument("--traffic-sample", type=int, default=3000, help="Traffic sample size")
    parser.add_argument("--aq-sample", type=int, default=3000, help="AQ sample size")
    parser.add_argument("--time-window-minutes", type=int, default=15, help="Temporal join window (+/- minutes)")
    parser.add_argument(
        "--distance-threshold-m",
        type=float,
        default=500.0,
        help="Distance threshold for geometric-style baseline (meters)",
    )
    parser.add_argument(
        "--bucket-seconds",
        type=int,
        default=300,
        help="Pre-join time bucket width to avoid full cartesian baseline join",
    )
    parser.add_argument(
        "--report-path",
        default="workspace/benchmarks/E3_m6_h3_vs_geometry_report.md",
        help="Path to write benchmark report markdown",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("E3_m6_h3_vs_geometry_benchmark")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "16")
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


def load_samples(spark: SparkSession, args: argparse.Namespace) -> tuple[DataFrame, DataFrame]:
    traffic = (
        spark.read.parquet(args.traffic_path)
        .select(col("id").alias("traffic_id"), "traffic_event_ts", "traffic_lat", "traffic_lon", "h3_index")
        .where(
            col("traffic_event_ts").isNotNull()
            & col("traffic_lat").isNotNull()
            & col("traffic_lon").isNotNull()
            & col("h3_index").isNotNull()
        )
    )
    aq = (
        spark.read.parquet(args.aq_path)
        .select("aq_location_id", "aq_event_ts", "aq_lat", "aq_lon", "h3_index")
        .where(
            col("aq_event_ts").isNotNull()
            & col("aq_lat").isNotNull()
            & col("aq_lon").isNotNull()
            & col("h3_index").isNotNull()
        )
    )

    if args.hours_back > 0:
        traffic = traffic.where(col("traffic_event_ts") >= expr(f"current_timestamp() - INTERVAL {args.hours_back} HOURS"))
        aq = aq.where(col("aq_event_ts") >= expr(f"current_timestamp() - INTERVAL {args.hours_back} HOURS"))

    traffic = traffic.orderBy(col("traffic_event_ts").desc()).limit(args.traffic_sample).cache()
    aq = aq.orderBy(col("aq_event_ts").desc()).limit(args.aq_sample).cache()

    # Materialize cache and report sample sizes.
    traffic_count = traffic.count()
    aq_count = aq.count()
    print(f"[sample] traffic_rows={traffic_count} aq_rows={aq_count}")
    return traffic, aq


def benchmark_h3(traffic: DataFrame, aq: DataFrame, args: argparse.Namespace) -> tuple[float, int]:
    window_seconds = args.time_window_minutes * 60
    joined = traffic.alias("t").join(
        aq.alias("a"),
        (
            (col("t.h3_index") == col("a.h3_index"))
            & (spark_abs(unix_timestamp(col("t.traffic_event_ts")) - unix_timestamp(col("a.aq_event_ts"))) <= window_seconds)
        ),
        "inner",
    )

    start = time.perf_counter()
    row_count = joined.count()
    elapsed_s = time.perf_counter() - start
    print(f"[bench] h3_join rows={row_count} elapsed_s={elapsed_s:.3f}")
    return elapsed_s, row_count


def benchmark_geometry(traffic: DataFrame, aq: DataFrame, args: argparse.Namespace) -> tuple[float, int]:
    window_seconds = args.time_window_minutes * 60
    distance_m = args.distance_threshold_m
    bucket = args.bucket_seconds
    max_bucket_delta = max(1, int(window_seconds / bucket))

    t = traffic.withColumn("t_bucket", floor(unix_timestamp(col("traffic_event_ts")) / bucket))
    a = aq.withColumn("a_bucket", floor(unix_timestamp(col("aq_event_ts")) / bucket))

    # Haversine in SQL expression form (meters).
    haversine_expr = """
        2 * 6371000 * asin(
            sqrt(
                pow(sin(radians(a.aq_lat - t.traffic_lat) / 2), 2) +
                cos(radians(t.traffic_lat)) * cos(radians(a.aq_lat)) *
                pow(sin(radians(a.aq_lon - t.traffic_lon) / 2), 2)
            )
        )
    """

    joined = t.alias("t").join(
        a.alias("a"),
        (
            (spark_abs(col("t.t_bucket") - col("a.a_bucket")) <= max_bucket_delta)
            & (spark_abs(unix_timestamp(col("t.traffic_event_ts")) - unix_timestamp(col("a.aq_event_ts"))) <= window_seconds)
            & (expr(haversine_expr) <= distance_m)
        ),
        "inner",
    )

    start = time.perf_counter()
    row_count = joined.count()
    elapsed_s = time.perf_counter() - start
    print(f"[bench] geometry_join rows={row_count} elapsed_s={elapsed_s:.3f}")
    return elapsed_s, row_count


def write_report(args: argparse.Namespace, h3_s: float, h3_rows: int, geo_s: float, geo_rows: int) -> None:
    report_path = Path(args.report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    speedup = (geo_s / h3_s) if h3_s > 0 else float("inf")
    generated_at = datetime.now(timezone.utc).isoformat()

    report = f"""## Epic 3 Milestone 6 Benchmark Report

Generated: `{generated_at}`

### Benchmark Config
- traffic_path: `{args.traffic_path}`
- aq_path: `{args.aq_path}`
- hours_back: `{args.hours_back}`
- traffic_sample: `{args.traffic_sample}`
- aq_sample: `{args.aq_sample}`
- time_window_minutes: `{args.time_window_minutes}`
- distance_threshold_m: `{args.distance_threshold_m}`
- bucket_seconds: `{args.bucket_seconds}`

### Results
- H3 join elapsed_s: `{h3_s:.3f}` (rows=`{h3_rows}`)
- Geometry-style join elapsed_s: `{geo_s:.3f}` (rows=`{geo_rows}`)
- Relative speedup (geometry/h3): `{speedup:.2f}x`

### Spark UI Metrics (manual capture)
Capture from Spark UI SQL tab for each benchmark query:

- H3 query
  - total_duration_s: `TODO`
  - total_tasks: `TODO`
  - shuffle_read_bytes: `TODO`
  - shuffle_write_bytes: `TODO`
- Geometry-style query
  - total_duration_s: `TODO`
  - total_tasks: `TODO`
  - shuffle_read_bytes: `TODO`
  - shuffle_write_bytes: `TODO`

### Interpretation
- If H3 query is materially faster and has lower shuffle, keep H3 strategy.
- If AQ match quality drops too much, tune H3 resolution or time window and rerun.

### H3 Resolution Tuning Note
- Increase resolution (smaller cells) when false spatial matches are too high.
- Decrease resolution (larger cells) when match sparsity is too high or AQ coverage drops.
- Re-evaluate whenever sensor density or target join radius changes.
"""
    report_path.write_text(report, encoding="utf-8")
    print(f"[ok] Wrote benchmark report template: {report_path}")


def main() -> int:
    args = parse_args()
    spark = build_spark()
    try:
        traffic, aq = load_samples(spark, args)
        if traffic.count() == 0 or aq.count() == 0:
            print("[error] Empty benchmark sample. Increase --hours-back or verify refined sinks.")
            return 2

        h3_s, h3_rows = benchmark_h3(traffic, aq, args)
        geo_s, geo_rows = benchmark_geometry(traffic, aq, args)
        write_report(args, h3_s, h3_rows, geo_s, geo_rows)

        if h3_s <= geo_s:
            print("[ok] H3 benchmark is faster/equal than geometry baseline on sampled workload.")
        else:
            print("[warn] Geometry baseline outperformed H3 on this sample; review config and data skew.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
