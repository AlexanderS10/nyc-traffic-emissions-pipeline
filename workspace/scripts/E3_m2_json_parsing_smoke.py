#!/usr/bin/env python3
"""
Milestone 2 smoke test for JSON parsing hardening.

This script validates:
1) Explicit schema parsing with from_json.
2) Malformed JSON detection by null payload split.
3) Required-column projection for downstream processing.

Run in container:
  docker exec -it jupyter-pyspark python /home/jovyan/work/scripts/E3_m2_json_parsing_smoke.py
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType


TRAFFIC_SCHEMA = StructType(
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
)

REQUIRED_COLUMNS = [
    "id",
    "status",
    "speed",
    "travel_time",
    "link_name",
    "borough",
    "from_street",
    "to_street",
    "data_as_of",
    "link_points",
    "encoded_poly_line",
]


def main() -> int:
    spark = (
        SparkSession.builder
        .appName("m2_json_parsing_smoke")
        .master("local[1]")
        .getOrCreate()
    )

    try:
        rows = [
            (
                '{"id":"1","status":"0","speed":"42","travel_time":"9","link_name":"A","borough":"Manhattan","from_street":"A","to_street":"B","data_as_of":"2026-04-25T20:00:00.000","link_points":"40.75,-73.98","encoded_poly_line":"abc"}',
                "2026-04-25T20:00:01Z",
            ),
            ("{bad json payload", "2026-04-25T20:00:02Z"),
        ]
        source_df = spark.createDataFrame(rows, ["raw_payload", "kafka_timestamp"])

        parser_schema = StructType(TRAFFIC_SCHEMA.fields + [StructField("_corrupt_json", StringType(), True)])

        parsed_df = source_df.select(
            col("raw_payload"),
            col("kafka_timestamp"),
            from_json(
                col("raw_payload"),
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
            col("kafka_timestamp"),
            col("raw_payload"),
        )
        projected_df = valid_df.select(*REQUIRED_COLUMNS, "kafka_timestamp")

        valid_count = valid_df.count()
        malformed_count = malformed_df.count()

        assert valid_count == 1, f"Expected 1 valid row, got {valid_count}"
        assert malformed_count == 1, f"Expected 1 malformed row, got {malformed_count}"
        assert projected_df.columns == REQUIRED_COLUMNS + ["kafka_timestamp"]

        print("[ok] Explicit schema parsing works with from_json.")
        print("[ok] Malformed JSON is separated into quarantine stream.")
        print("[ok] Required-column projection is enforced.")
        print("[ok] Milestone 2 JSON parsing smoke test passed.")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
