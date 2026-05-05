#!/usr/bin/env python3
"""
Verify malformed-record quarantine output in MinIO.

This script checks the latest record under:
  s3a://raw-data/quarantine/malformed_json/

Equivalent S3 path:
  s3://raw-data/quarantine/malformed_json/

Run:
  docker exec -it jupyter-pyspark python /home/jovyan/work/scripts/E3_m2_verify_quarantine_record.py
"""

from __future__ import annotations

import argparse
import sys

import boto3
import pyarrow.parquet as pq
import s3fs


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify malformed JSON quarantine records")
    parser.add_argument("--endpoint", default="http://minio:9000", help="MinIO S3 endpoint URL")
    parser.add_argument("--access-key", default="admin", help="MinIO access key")
    parser.add_argument("--secret-key", default="password", help="MinIO secret key")
    parser.add_argument("--bucket", default="raw-data", help="S3 bucket name")
    parser.add_argument(
        "--prefix",
        default="quarantine/malformed_json/",
        help="S3 prefix where malformed records are written",
    )
    parser.add_argument(
        "--expect-substring",
        default='{"bad_json": true',
        help="Optional substring expected in raw_payload",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    client = boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name="us-east-1",
    )
    fs = s3fs.S3FileSystem(
        client_kwargs={"endpoint_url": args.endpoint, "region_name": "us-east-1"},
        key=args.access_key,
        secret=args.secret_key,
    )

    response = client.list_objects_v2(Bucket=args.bucket, Prefix=args.prefix, MaxKeys=1000)
    objects = response.get("Contents", [])
    parquet_keys = [obj["Key"] for obj in objects if obj["Key"].endswith(".parquet")]

    if not parquet_keys:
        print("[error] No parquet files found in malformed quarantine path.", file=sys.stderr)
        print(f"[info] Checked: s3://{args.bucket}/{args.prefix}", file=sys.stderr)
        return 1

    selected_uri = None
    table = None
    empty_files = 0
    unreadable_files = 0

    for key in sorted(parquet_keys, reverse=True):
        candidate_uri = f"s3://{args.bucket}/{key}"
        try:
            candidate_table = pq.read_table(candidate_uri, filesystem=fs)
        except Exception as exc:
            unreadable_files += 1
            print(f"[warn] Could not read parquet file {candidate_uri}: {exc}", file=sys.stderr)
            continue

        if candidate_table.num_rows == 0:
            empty_files += 1
            continue

        selected_uri = candidate_uri
        table = candidate_table
        break

    if selected_uri is None or table is None:
        print(
            "[error] No non-empty readable parquet files found in malformed quarantine path.",
            file=sys.stderr,
        )
        print(
            f"[info] Checked: s3://{args.bucket}/{args.prefix} "
            f"(empty={empty_files}, unreadable={unreadable_files})",
            file=sys.stderr,
        )
        return 1

    print(f"[ok] Latest non-empty quarantine parquet: {selected_uri}")

    row = table.slice(table.num_rows - 1, 1).to_pylist()[0]
    print("[ok] Latest quarantined record:")
    print(f"  source_topic: {row.get('source_topic')}")
    print(f"  kafka_timestamp: {row.get('kafka_timestamp')}")
    print(f"  raw_payload: {row.get('raw_payload')}")

    expected = args.expect_substring
    if expected and expected not in str(row.get("raw_payload", "")):
        print(
            f"[warn] Latest raw_payload does not contain expected substring: {expected}",
            file=sys.stderr,
        )
        return 2

    print("[ok] Expected malformed payload marker found in quarantine output.")
    print("[ok] Quarantine verification passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
