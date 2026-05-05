#!/usr/bin/env python3
"""
Milestone 2 console probe for schema parsing and malformed routing.

This script reads one Kafka topic, parses JSON using permissive mode, and
writes both valid and malformed streams to console for quick verification.

Example:
  docker exec -it -w /home/jovyan/work jupyter-pyspark \
    python scripts/E3_m2_console_parsing_probe.py --topic nyc_traffic_raw --duration-seconds 45
"""

from __future__ import annotations

import argparse
import time
from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_SQL_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2"

SCHEMAS: dict[str, StructType] = {
    "nyc_traffic_raw": StructType(
        [
            StructField("id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("speed", StringType(), True),
            StructField("travel_time", StringType(), True),
            StructField("link_name", StringType(), True),
            StructField("borough", StringType(), True),
            StructField("from_street", StringType(), True),
            StructField("to_street", StringType(), True),
            StructField("data_as_of", StringType(), True),
            StructField("link_points", StringType(), True),
            StructField("encoded_poly_line", StringType(), True),
        ]
    ),
    "nyc_openaq_raw": StructType(
        [
            StructField("sensor_id", StringType(), True),
            StructField("source", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("pm25", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("location_id", LongType(), True),
            StructField("location_name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("parameter", StringType(), True),
            StructField("value", DoubleType(), True),
            StructField("unit", StringType(), True),
            StructField("datetime_utc", StringType(), True),
        ]
    ),
    "nyc_purpleair_raw": StructType(
        [
            StructField("sensor_id", StringType(), True),
            StructField("source", StringType(), True),
            StructField("lat", DoubleType(), True),
            StructField("lon", DoubleType(), True),
            StructField("pm25", DoubleType(), True),
            StructField("timestamp", StringType(), True),
            StructField("sensor_index", LongType(), True),
            StructField("name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("pm2.5_10minute", DoubleType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
        ]
    ),
    "nyc_weather_raw": StructType(
        [
            StructField("number", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("startTime", StringType(), True),
            StructField("endTime", StringType(), True),
            StructField("isDaytime", BooleanType(), True),
            StructField("temperature", IntegerType(), True),
            StructField("temperatureUnit", StringType(), True),
            StructField("temperatureTrend", StringType(), True),
            StructField("windSpeed", StringType(), True),
            StructField("windDirection", StringType(), True),
            StructField("shortForecast", StringType(), True),
            StructField("detailedForecast", StringType(), True),
            StructField(
                "probabilityOfPrecipitation",
                StructType(
                    [
                        StructField("unitCode", StringType(), True),
                        StructField("value", IntegerType(), True),
                    ]
                ),
                True,
            ),
        ]
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Milestone 2 JSON parsing console probe")
    parser.add_argument(
        "--master",
        default="local[2]",
        help="Spark master URL (default local[2] to avoid cluster-worker dependency)",
    )
    parser.add_argument("--bootstrap", default="redpanda:29092", help="Kafka bootstrap server")
    parser.add_argument(
        "--topic",
        default="nyc_traffic_raw",
        choices=sorted(SCHEMAS.keys()),
        help="Topic to probe",
    )
    parser.add_argument(
        "--starting-offsets",
        default="latest",
        choices=("latest", "earliest"),
        help="Kafka starting offsets",
    )
    parser.add_argument("--duration-seconds", type=int, default=45, help="How long to run probe")
    parser.add_argument("--num-rows", type=int, default=20, help="Console rows per micro-batch")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = uuid4().hex[:10]
    checkpoint_base = f"/tmp/e3_m2_console_probe_{args.topic}_{run_id}"
    schema = SCHEMAS[args.topic]
    parser_schema = StructType(schema.fields + [StructField("_corrupt_json", StringType(), True)])

    spark = (
        SparkSession.builder
        .appName("E3_m2_console_parsing_probe")
        .master(args.master)
        .config("spark.jars.packages", KAFKA_SQL_PKG)
        .config("spark.driver.host", "jupyter-pyspark")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    print(
        f"[info] Starting probe for topic={args.topic} bootstrap={args.bootstrap} master={args.master} "
        f"offsets={args.starting_offsets}"
    )
    print(f"[info] Console probe runtime={args.duration_seconds}s checkpoints={checkpoint_base}")

    source_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = source_df.select(
        col("value").cast("string").alias("raw_payload"),
        col("timestamp").alias("kafka_timestamp"),
        lit(args.topic).alias("source_topic"),
        from_json(
            col("value").cast("string"),
            parser_schema,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_json"},
        ).alias("payload"),
    )

    valid_df = (
        parsed_df.where(col("payload._corrupt_json").isNull())
        .select("payload.*", "kafka_timestamp")
        .drop("_corrupt_json")
    )

    malformed_df = parsed_df.where(col("payload._corrupt_json").isNotNull()).select(
        "source_topic",
        "kafka_timestamp",
        "raw_payload",
    )

    valid_query = (
        valid_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", str(args.num_rows))
        .option("checkpointLocation", f"{checkpoint_base}/valid")
        .queryName(f"e3_m2_valid_{run_id}")
        .start()
    )

    malformed_query = (
        malformed_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", str(args.num_rows))
        .option("checkpointLocation", f"{checkpoint_base}/malformed")
        .queryName(f"e3_m2_malformed_{run_id}")
        .start()
    )

    try:
        deadline = time.time() + args.duration_seconds
        while time.time() < deadline:
            if not valid_query.isActive or not malformed_query.isActive:
                raise RuntimeError("Console probe query stopped unexpectedly.")
            time.sleep(1.0)
    finally:
        for query in (valid_query, malformed_query):
            if query.isActive:
                query.stop()
        spark.stop()

    print("[ok] Console probe finished. Review output above for typed columns and malformed rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
