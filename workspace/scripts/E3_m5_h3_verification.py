#!/usr/bin/env python3
"""
Milestone 5 verification helper: H3 format sanity + nearby-cell behavior.

Checks:
1) Valid-looking and valid H3 indexes on both refined streams.
2) H3 resolution matches expected value.
3) Nearby-point sanity: physically close points tend to map to same/neighboring H3 cells.

Run in container:
  docker exec -it -w /home/jovyan/work jupyter-pyspark \
    python scripts/E3_m5_h3_verification.py
"""

from __future__ import annotations

import math
import re
import sys
from dataclasses import dataclass

import h3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

EXPECTED_H3_RESOLUTION = 7
H3_HEX_RE = re.compile(r"^[0-9a-f]{15}$")


@dataclass
class PointRow:
    lat: float
    lon: float
    h3_index: str


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("E3_m5_h3_verification")
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


def evaluate_h3_columns(df, lat_col: str, lon_col: str, label: str) -> tuple[int, int, int, int]:
    rows = (
        df.select(col(lat_col).alias("lat"), col(lon_col).alias("lon"), col("h3_index"))
        .where(col("h3_index").isNotNull() & col("lat").isNotNull() & col("lon").isNotNull())
        .limit(5000)
        .collect()
    )

    total = len(rows)
    format_valid = 0
    cell_valid = 0
    res_valid = 0

    for row in rows:
        idx = str(row["h3_index"]).lower()
        if H3_HEX_RE.match(idx):
            format_valid += 1
        if h3.is_valid_cell(idx):
            cell_valid += 1
            if h3.get_resolution(idx) == EXPECTED_H3_RESOLUTION:
                res_valid += 1

    print(
        f"[h3] {label}: sampled={total} format_valid={format_valid} "
        f"cell_valid={cell_valid} resolution_{EXPECTED_H3_RESOLUTION}={res_valid}"
    )
    return total, format_valid, cell_valid, res_valid


def nearest_neighbor_sanity(df, lat_col: str, lon_col: str, label: str) -> tuple[int, int]:
    sampled_rows = (
        df.select(col(lat_col).alias("lat"), col(lon_col).alias("lon"), col("h3_index"))
        .where(col("h3_index").isNotNull() & col("lat").isNotNull() & col("lon").isNotNull())
        .limit(300)
        .collect()
    )
    points = [PointRow(float(r["lat"]), float(r["lon"]), str(r["h3_index"]).lower()) for r in sampled_rows]

    if len(points) < 2:
        print(f"[nearby] {label}: insufficient rows for nearest-neighbor sanity.")
        return 0, 0

    evaluated = 0
    good = 0
    for i, point in enumerate(points):
        nearest_idx = None
        nearest_dist = float("inf")
        for j, other in enumerate(points):
            if i == j:
                continue
            d = math.hypot(point.lat - other.lat, point.lon - other.lon)
            if d < nearest_dist:
                nearest_dist = d
                nearest_idx = j

        if nearest_idx is None:
            continue

        other = points[nearest_idx]
        evaluated += 1
        if point.h3_index == other.h3_index:
            good += 1
            continue

        try:
            grid_dist = h3.grid_distance(point.h3_index, other.h3_index)
        except Exception:
            grid_dist = None
        if grid_dist is not None and grid_dist <= 1:
            good += 1

    print(f"[nearby] {label}: evaluated={evaluated} same_or_neighbor={good}")
    return evaluated, good


def main() -> int:
    spark = build_spark()
    try:
        traffic_refined = spark.read.parquet("s3a://refined-data/traffic/")
        aq_refined = spark.read.parquet("s3a://refined-data/air_quality/")

        t_total, t_fmt, t_cell, t_res = evaluate_h3_columns(
            traffic_refined, "traffic_lat", "traffic_lon", "traffic_refined"
        )
        a_total, a_fmt, a_cell, a_res = evaluate_h3_columns(
            aq_refined, "aq_lat", "aq_lon", "air_quality_refined"
        )

        t_eval, t_good = nearest_neighbor_sanity(traffic_refined, "traffic_lat", "traffic_lon", "traffic_refined")
        a_eval, a_good = nearest_neighbor_sanity(aq_refined, "aq_lat", "aq_lon", "air_quality_refined")

        print("[sample] traffic h3_index values")
        traffic_refined.select("h3_index").where(col("h3_index").isNotNull()).limit(10).show(truncate=False)
        print("[sample] air quality h3_index values")
        aq_refined.select("h3_index").where(col("h3_index").isNotNull()).limit(10).show(truncate=False)

        if min(t_total, a_total) == 0:
            print("[error] Missing h3_index samples in one or both refined streams.")
            return 2
        if min(t_fmt, t_cell, t_res) == 0 or min(a_fmt, a_cell, a_res) == 0:
            print("[error] H3 format/validity/resolution checks failed.")
            return 3
        if (t_eval > 0 and t_good == 0) or (a_eval > 0 and a_good == 0):
            print("[error] Nearby-point sanity check failed (no same/neighboring mappings observed).")
            return 4

        print("[ok] Milestone 5 H3 verification passed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        raise SystemExit(1)
