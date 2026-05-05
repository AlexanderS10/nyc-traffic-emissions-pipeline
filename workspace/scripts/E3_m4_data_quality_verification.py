#!/usr/bin/env python3
"""
Milestone 4 verification helper: data quality filters and NYC bounds checks.

This script validates that configured quality filters eliminate invalid records
by computing:
1) pre-filter invalid counts by rule
2) post-filter invalid counts (should be zero)
3) cleaned sample rows (console-style inspection)

Run in container:
  docker exec -it -w /home/jovyan/work jupyter-pyspark \
    python scripts/E3_m4_data_quality_verification.py
"""

from __future__ import annotations

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, expr
from pyspark.sql.types import DoubleType, TimestampType

NYC_LAT_MIN, NYC_LAT_MAX = 40.4774, 40.9176
NYC_LON_MIN, NYC_LON_MAX = -74.2591, -73.7004
TRAFFIC_SPEED_MAX_MPH = 100.0
AQ_PM25_MAX_UGM3 = 2000.0


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("E3_m4_data_quality_verification")
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


def main() -> int:
    spark = build_spark()
    try:
        traffic_raw = spark.read.parquet("s3a://raw-data/traffic/")
        openaq_raw = spark.read.parquet("s3a://raw-data/openaq/")
        purpleair_raw = spark.read.parquet("s3a://raw-data/purpleair/")

        # Keep parsing and filtering aligned with notebook data-quality logic.
        traffic_parsed = (
            traffic_raw
            .withColumn("speed", col("speed").cast(DoubleType()))
            .withColumn("traffic_event_ts", col("data_as_of").cast(TimestampType()))
            .withColumn("first_link_point", expr("trim(element_at(split(link_points, ' '), 1))"))
            .withColumn("traffic_lat", expr("cast(element_at(split(first_link_point, ','), 1) as double)"))
            .withColumn("traffic_lon", expr("cast(element_at(split(first_link_point, ','), 2) as double)"))
        )

        aq_open = (
            openaq_raw
            .withColumn("aq_event_ts", coalesce(col("timestamp").cast(TimestampType()), col("datetime_utc").cast(TimestampType())))
            .withColumn("aq_lat", coalesce(col("lat").cast(DoubleType()), col("latitude").cast(DoubleType())))
            .withColumn("aq_lon", coalesce(col("lon").cast(DoubleType()), col("longitude").cast(DoubleType())))
            .withColumn("aq_pm25_ugm3", coalesce(col("pm25").cast(DoubleType()), col("value").cast(DoubleType())))
            .withColumn("aq_location_id", coalesce(col("sensor_id"), col("location_id").cast("string"), col("location_name")))
        )
        aq_purple = (
            purpleair_raw
            .withColumn("aq_event_ts", coalesce(col("timestamp").cast(TimestampType()), col("kafka_timestamp").cast(TimestampType())))
            .withColumn("aq_lat", coalesce(col("lat").cast(DoubleType()), col("latitude").cast(DoubleType())))
            .withColumn("aq_lon", coalesce(col("lon").cast(DoubleType()), col("longitude").cast(DoubleType())))
            .withColumn("aq_pm25_ugm3", coalesce(col("pm25").cast(DoubleType()), col("`pm2.5_10minute`").cast(DoubleType())))
            .withColumn("aq_location_id", coalesce(col("sensor_id"), col("name")))
        )
        aq_union = aq_open.select("aq_location_id", "aq_event_ts", "aq_lat", "aq_lon", "aq_pm25_ugm3").unionByName(
            aq_purple.select("aq_location_id", "aq_event_ts", "aq_lat", "aq_lon", "aq_pm25_ugm3"),
            allowMissingColumns=True,
        )

        traffic_total = traffic_parsed.count()
        aq_total = aq_union.count()

        traffic_invalid_speed = traffic_parsed.where(
            col("speed").isNull() | ~col("speed").between(0.0, TRAFFIC_SPEED_MAX_MPH)
        ).count()
        traffic_invalid_coords = traffic_parsed.where(
            col("traffic_lat").isNull()
            | col("traffic_lon").isNull()
            | (col("traffic_lat") < NYC_LAT_MIN)
            | (col("traffic_lat") > NYC_LAT_MAX)
            | (col("traffic_lon") < NYC_LON_MIN)
            | (col("traffic_lon") > NYC_LON_MAX)
        ).count()
        traffic_null_critical = traffic_parsed.where(
            col("id").isNull() | col("traffic_event_ts").isNull()
        ).count()

        aq_invalid_pm25 = aq_union.where(
            col("aq_pm25_ugm3").isNull() | ~col("aq_pm25_ugm3").between(0.0, AQ_PM25_MAX_UGM3)
        ).count()
        aq_invalid_coords = aq_union.where(
            col("aq_lat").isNull()
            | col("aq_lon").isNull()
            | (col("aq_lat") < NYC_LAT_MIN)
            | (col("aq_lat") > NYC_LAT_MAX)
            | (col("aq_lon") < NYC_LON_MIN)
            | (col("aq_lon") > NYC_LON_MAX)
        ).count()
        aq_null_critical = aq_union.where(
            col("aq_location_id").isNull() | col("aq_event_ts").isNull()
        ).count()

        traffic_clean = (
            traffic_parsed
            .where(col("id").isNotNull() & col("traffic_event_ts").isNotNull())
            .where(col("traffic_lat").isNotNull() & col("traffic_lon").isNotNull())
            .where(
                (col("traffic_lat") >= NYC_LAT_MIN)
                & (col("traffic_lat") <= NYC_LAT_MAX)
                & (col("traffic_lon") >= NYC_LON_MIN)
                & (col("traffic_lon") <= NYC_LON_MAX)
            )
            .where(col("speed").isNotNull() & col("speed").between(0.0, TRAFFIC_SPEED_MAX_MPH))
        )
        aq_clean = (
            aq_union
            .where(col("aq_location_id").isNotNull() & col("aq_event_ts").isNotNull())
            .where(col("aq_lat").isNotNull() & col("aq_lon").isNotNull())
            .where(
                (col("aq_lat") >= NYC_LAT_MIN)
                & (col("aq_lat") <= NYC_LAT_MAX)
                & (col("aq_lon") >= NYC_LON_MIN)
                & (col("aq_lon") <= NYC_LON_MAX)
            )
            .where(col("aq_pm25_ugm3").isNotNull() & col("aq_pm25_ugm3").between(0.0, AQ_PM25_MAX_UGM3))
        )

        print("[metrics] Pre-filter invalid counts")
        print(
            f"  traffic_total={traffic_total} traffic_invalid_speed={traffic_invalid_speed} "
            f"traffic_invalid_coords={traffic_invalid_coords} traffic_null_critical={traffic_null_critical}"
        )
        print(
            f"  aq_total={aq_total} aq_invalid_pm25={aq_invalid_pm25} "
            f"aq_invalid_coords={aq_invalid_coords} aq_null_critical={aq_null_critical}"
        )

        print("[metrics] Post-filter invalid counts (should be zero)")
        post_traffic_invalid = traffic_clean.where(
            col("speed").isNull()
            | ~col("speed").between(0.0, TRAFFIC_SPEED_MAX_MPH)
            | col("traffic_lat").isNull()
            | col("traffic_lon").isNull()
            | (col("traffic_lat") < NYC_LAT_MIN)
            | (col("traffic_lat") > NYC_LAT_MAX)
            | (col("traffic_lon") < NYC_LON_MIN)
            | (col("traffic_lon") > NYC_LON_MAX)
            | col("id").isNull()
            | col("traffic_event_ts").isNull()
        ).count()
        post_aq_invalid = aq_clean.where(
            col("aq_pm25_ugm3").isNull()
            | ~col("aq_pm25_ugm3").between(0.0, AQ_PM25_MAX_UGM3)
            | col("aq_lat").isNull()
            | col("aq_lon").isNull()
            | (col("aq_lat") < NYC_LAT_MIN)
            | (col("aq_lat") > NYC_LAT_MAX)
            | (col("aq_lon") < NYC_LON_MIN)
            | (col("aq_lon") > NYC_LON_MAX)
            | col("aq_location_id").isNull()
            | col("aq_event_ts").isNull()
        ).count()
        print(f"  traffic_post_invalid={post_traffic_invalid}")
        print(f"  aq_post_invalid={post_aq_invalid}")

        print("[sample] Clean traffic rows")
        traffic_clean.select("id", "speed", "traffic_lat", "traffic_lon", "traffic_event_ts").show(10, truncate=False)
        print("[sample] Clean AQ rows")
        aq_clean.select("aq_location_id", "aq_pm25_ugm3", "aq_lat", "aq_lon", "aq_event_ts").show(10, truncate=False)

        if post_traffic_invalid != 0 or post_aq_invalid != 0:
            print("[error] Post-filter invalid rows remain; inspect metrics above.")
            return 2

        print("[ok] Milestone 4 quality filter verification passed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        raise SystemExit(1)
