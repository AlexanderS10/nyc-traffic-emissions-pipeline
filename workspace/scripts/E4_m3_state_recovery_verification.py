#!/usr/bin/env python3
"""
Epic 4 Milestone 3 verification helper.

Checks:
1) Stateful join plan includes StreamingSymmetricHashJoin.
2) Configured business checkpoint path has artifacts.
3) Stream can be stopped and restarted against the same checkpoint without crashing.
"""

from __future__ import annotations

import argparse
import io
from contextlib import redirect_stdout

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Epic 4 Milestone 3 stateful join/checkpoint behavior")
    parser.add_argument("--traffic-path", default="s3a://refined-data/traffic/", help="Refined traffic source path")
    parser.add_argument("--aq-path", default="s3a://refined-data/air_quality/", help="Refined AQ source path")
    parser.add_argument(
        "--business-checkpoint",
        default="s3a://business-data/checkpoints/local.db.enriched_traffic_v4",
        help="Configured business checkpoint path to verify",
    )
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=15,
        help="Join correlation window in minutes",
    )
    parser.add_argument(
        "--smoke-output-path",
        default="s3a://business-data/tmp/e4_m3_recovery_smoke",
        help="Output path used for restart smoke test",
    )
    parser.add_argument(
        "--smoke-checkpoint-path",
        default="s3a://business-data/checkpoints/e4_m3_recovery_smoke",
        help="Checkpoint path used for restart smoke test",
    )
    parser.add_argument(
        "--run-seconds",
        type=int,
        default=15,
        help="Seconds to let each smoke query run before stop",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("E4_m3_state_recovery_verification")
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


def build_streaming_join(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    traffic_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("h3_index", StringType(), True),
            StructField("traffic_event_ts", TimestampType(), True),
            StructField("speed", DoubleType(), True),
        ]
    )
    aq_schema = StructType(
        [
            StructField("aq_location_id", StringType(), True),
            StructField("h3_index", StringType(), True),
            StructField("aq_event_ts", TimestampType(), True),
            StructField("aq_pm25_ugm3", DoubleType(), True),
        ]
    )

    traffic = (
        spark.readStream.format("parquet")
        .schema(traffic_schema)
        .option("maxFilesPerTrigger", 2)
        .load(args.traffic_path)
        .where(
            col("traffic_event_ts").isNotNull()
            & col("h3_index").isNotNull()
            & col("id").isNotNull()
        )
        .withWatermark("traffic_event_ts", f"{args.window_minutes} minutes")
    )

    aq = (
        spark.readStream.format("parquet")
        .schema(aq_schema)
        .option("maxFilesPerTrigger", 2)
        .load(args.aq_path)
        .where(
            col("aq_event_ts").isNotNull()
            & col("h3_index").isNotNull()
            & col("aq_location_id").isNotNull()
            & col("aq_pm25_ugm3").isNotNull()
        )
        .withWatermark("aq_event_ts", f"{args.window_minutes} minutes")
    )

    return (
        traffic.alias("t")
        .join(
            aq.alias("aq"),
            (
                (col("t.h3_index") == col("aq.h3_index"))
                & (
                    col("aq.aq_event_ts")
                    >= col("t.traffic_event_ts") - expr(f"INTERVAL {args.window_minutes} MINUTES")
                )
                & (
                    col("aq.aq_event_ts")
                    <= col("t.traffic_event_ts") + expr(f"INTERVAL {args.window_minutes} MINUTES")
                )
            ),
            "inner",
        )
        .select(
            col("t.id").alias("traffic_id"),
            col("t.h3_index"),
            col("t.traffic_event_ts"),
            col("aq.aq_event_ts"),
            col("t.speed").alias("traffic_speed"),
            col("aq.aq_pm25_ugm3"),
        )
    )


def hadoop_path_exists_with_children(spark: SparkSession, path_str: str) -> tuple[bool, int]:
    jvm = spark._jvm
    jsc = spark._jsc
    hconf = jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(hconf)
    exists = fs.exists(path)
    if not exists:
        return False, 0
    children = fs.listStatus(path)
    return True, len(children)


def clear_path_if_exists(spark: SparkSession, path_str: str) -> None:
    jvm = spark._jvm
    jsc = spark._jsc
    hconf = jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(path_str)
    fs = path.getFileSystem(hconf)
    if fs.exists(path):
        fs.delete(path, True)


def run_restart_smoke_test(spark: SparkSession, joined: DataFrame, args: argparse.Namespace) -> None:
    clear_path_if_exists(spark, args.smoke_output_path)
    clear_path_if_exists(spark, args.smoke_checkpoint_path)

    q1 = (
        joined.writeStream.format("parquet")
        .option("path", args.smoke_output_path)
        .option("checkpointLocation", args.smoke_checkpoint_path)
        .trigger(processingTime="5 seconds")
        .start()
    )
    try:
        q1.awaitTermination(args.run_seconds)
    finally:
        q1.stop()

    q2 = (
        joined.writeStream.format("parquet")
        .option("path", args.smoke_output_path)
        .option("checkpointLocation", args.smoke_checkpoint_path)
        .trigger(processingTime="5 seconds")
        .start()
    )
    try:
        q2.awaitTermination(args.run_seconds)
    finally:
        q2.stop()


def main() -> int:
    args = parse_args()
    spark = build_spark()
    try:
        joined = build_streaming_join(spark, args)

        explain_capture = io.StringIO()
        with redirect_stdout(explain_capture):
            joined.explain("extended")
        plan_text = explain_capture.getvalue()
        has_stateful_join = any(
            token in plan_text
            for token in (
                "StreamingSymmetricHashJoin",
                "StreamingSymmetricHashJoinExec",
                "symmetricHashJoin",
            )
        )
        print(f"[check] has_streaming_symmetric_hash_join={has_stateful_join}")
        if not has_stateful_join:
            print("[fail] Join plan did not include StreamingSymmetricHashJoin.")
            return 1

        exists, child_count = hadoop_path_exists_with_children(spark, args.business_checkpoint)
        print(f"[check] business_checkpoint_exists={exists} child_entries={child_count}")
        if not exists or child_count == 0:
            print("[fail] Configured business checkpoint path missing or empty.")
            return 1

        run_restart_smoke_test(spark, joined, args)
        smoke_exists, smoke_children = hadoop_path_exists_with_children(spark, args.smoke_checkpoint_path)
        print(f"[check] smoke_checkpoint_exists={smoke_exists} child_entries={smoke_children}")
        if not smoke_exists or smoke_children == 0:
            print("[fail] Restart smoke checkpoint was not created.")
            return 1

        print("[ok] Epic 4 Milestone 3 verification checks passed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
