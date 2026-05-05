#!/usr/bin/env python3
"""
Epic 4 Milestone 5 verification helper.

Checks:
1) Build an H3-keyed static lookup from raw traffic seed keys + static datasets.
2) Print total/flagged H3 counts.
3) Show sample lookup rows and elapsed runtime.
"""

from __future__ import annotations

import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, col, expr, udf
from pyspark.sql.types import StringType

import h3


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Epic 4 Milestone 5 static H3 lookup generation")
    parser.add_argument("--traffic-path", default="s3a://raw-data/traffic/", help="Raw traffic parquet path")
    parser.add_argument("--h3-resolution", type=int, default=7, help="H3 resolution for lookup keys")
    parser.add_argument("--sample", type=int, default=10, help="Sample lookup rows to print")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("E4_m5_static_lookup_verification")
        .master("local[2]")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,"
            "org.apache.sedona:sedona-spark-shaded-4.0_2.13:1.8.1,"
            "org.datasyslab:geotools-wrapper:1.8.1-33.1,"
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "software.amazon.awssdk:bundle:2.29.38",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.sedona.sql.SedonaSqlExtensions",
        )
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def latlon_to_h3_factory(resolution: int):
    @udf(StringType())
    def _latlon_to_h3(lat, lon):
        if lat is None or lon is None:
            return None
        try:
            return h3.latlng_to_cell(float(lat), float(lon), resolution)
        except Exception:
            return None

    return _latlon_to_h3


def main() -> int:
    args = parse_args()
    spark = build_spark()
    start_ts = time.time()

    try:
        latlon_to_h3 = latlon_to_h3_factory(args.h3_resolution)
        nyc_lat_min, nyc_lat_max = 40.4774, 40.9176
        nyc_lon_min, nyc_lon_max = -74.2591, -73.7004

        congestion_df = (
            spark.read.option("multiline", "true").json("s3a://raw-data/static/congestion_zones/")
            .selectExpr("explode(features) as feature")
            .selectExpr("ST_GeomFromGeoJSON(to_json(feature.geometry)) as zone_geom")
        )

        truck_routes_df = (
            spark.read.format("shapefile")
            .load("s3a://raw-data/static/truck_routes/")
            .selectExpr("geometry as route_geom")
        )

        ll84_df = (
            spark.read.option("header", "true").csv("s3a://raw-data/static/building_energy/*.csv")
            .filter(col("Latitude").isNotNull() & col("Longitude").isNotNull())
            .selectExpr(
                "ST_Point(cast(Longitude as double), cast(Latitude as double)) as bldg_geom",
                "TRY_CAST(`Total GHG Emissions (Metric Tons CO2e)` as double) as bldg_ghg_emissions",
            )
            .filter(col("bldg_ghg_emissions") > 10000.0)
            .select("bldg_geom")
        )

        h3_seed_df = (
            spark.read.parquet(args.traffic_path)
            .selectExpr("trim(element_at(split(link_points, ' '), 1)) as first_link_point")
            .selectExpr(
                "cast(element_at(split(first_link_point, ','), 1) as double) as traffic_lat",
                "cast(element_at(split(first_link_point, ','), 2) as double) as traffic_lon",
            )
            .filter(col("traffic_lat").isNotNull() & col("traffic_lon").isNotNull())
            .filter(
                (col("traffic_lat") >= nyc_lat_min)
                & (col("traffic_lat") <= nyc_lat_max)
                & (col("traffic_lon") >= nyc_lon_min)
                & (col("traffic_lon") <= nyc_lon_max)
            )
            .withColumn("h3_index", latlon_to_h3(col("traffic_lat"), col("traffic_lon")))
            .filter(col("h3_index").isNotNull())
            .dropDuplicates(["h3_index"])
            .withColumn("h3_point_geom", expr("ST_Point(traffic_lon, traffic_lat)"))
        )

        lookup_df = (
            h3_seed_df.alias("h")
            .join(broadcast(congestion_df.alias("c")), expr("ST_Contains(c.zone_geom, h.h3_point_geom)"), "leftOuter")
            .join(broadcast(truck_routes_df.alias("r")), expr("ST_Distance(r.route_geom, h.h3_point_geom) < 0.001"), "leftOuter")
            .join(broadcast(ll84_df.alias("b")), expr("ST_Distance(b.bldg_geom, h.h3_point_geom) < 0.001"), "leftOuter")
            .groupBy("h.h3_index")
            .agg(
                expr("max(case when c.zone_geom is not null then true else false end)").alias("is_in_congestion_zone"),
                expr("max(case when r.route_geom is not null then true else false end)").alias("is_near_truck_route"),
                expr("max(case when b.bldg_geom is not null then true else false end)").alias("is_near_high_emission_bldg"),
            )
            .cache()
        )

        total_h3 = lookup_df.count()
        congestion_h3 = lookup_df.filter(col("is_in_congestion_zone")).count()
        truck_h3 = lookup_df.filter(col("is_near_truck_route")).count()
        bldg_h3 = lookup_df.filter(col("is_near_high_emission_bldg")).count()
        elapsed = time.time() - start_ts

        print(f"[check] total_h3_keys={total_h3}")
        print(f"[check] congestion_h3_keys={congestion_h3}")
        print(f"[check] truck_route_h3_keys={truck_h3}")
        print(f"[check] high_emission_bldg_h3_keys={bldg_h3}")
        print(f"[check] build_elapsed_seconds={elapsed:.2f}")
        print("[sample] lookup rows:")
        lookup_df.orderBy(col("h3_index")).show(args.sample, truncate=False)

        if total_h3 == 0:
            print("[fail] No H3 lookup keys were generated.")
            return 1
        print("[ok] Epic 4 Milestone 5 verification checks passed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
