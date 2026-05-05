#!/usr/bin/env python3
"""
Download static municipal datasets and upload them to MinIO.

Uploads into:
  s3://raw-data/static/truck_routes/
  s3://raw-data/static/congestion_zones/
  s3://raw-data/static/building_energy/

Run (inside container):
  docker exec -it jupyter-pyspark python /home/jovyan/work/scripts/E2_initialize_static_data.py
"""

from __future__ import annotations

import argparse
import sys
import tempfile
import zipfile
from pathlib import Path

import boto3
import requests
from botocore.exceptions import ClientError


# Official Open Data download endpoints.
TRUCK_ROUTES_SHP_ZIP_URL = "https://data.cityofnewyork.us/api/geospatial/wnu3-egq7?method=export&format=Shapefile"
CONGESTION_ZONES_GEOJSON_URL = "https://data.ny.gov/api/views/srxy-5nxn/rows.geojson?accessType=DOWNLOAD"
LL84_CSV_URL = "https://data.cityofnewyork.us/api/views/usc3-8zwd/rows.csv?accessType=DOWNLOAD"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize static data in MinIO")
    parser.add_argument("--endpoint", default="http://minio:9000", help="MinIO endpoint URL")
    parser.add_argument("--access-key", default="admin", help="MinIO access key")
    parser.add_argument("--secret-key", default="password", help="MinIO secret key")
    parser.add_argument("--bucket", default="raw-data", help="MinIO bucket")
    parser.add_argument("--prefix", default="static", help="Prefix inside bucket")
    parser.add_argument(
        "--local-static-dir",
        default="/home/jovyan/work/static",
        help="Local fallback directory with subfolders truck_routes, congestion_zones, building_energy",
    )
    parser.add_argument(
        "--mode",
        choices=("remote", "local", "auto"),
        default="auto",
        help="remote=download only, local=local files only, auto=remote then local fallback",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing objects in MinIO (default: skip existing keys)",
    )
    parser.add_argument("--timeout", type=int, default=45, help="HTTP timeout seconds")
    return parser.parse_args()


def s3_client(args: argparse.Namespace):
    return boto3.client(
        "s3",
        endpoint_url=args.endpoint,
        aws_access_key_id=args.access_key,
        aws_secret_access_key=args.secret_key,
        region_name="us-east-1",
    )


def object_exists(client, bucket: str, key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code")
        if code in {"404", "NoSuchKey"}:
            return False
        raise


def upload_file_path(client, bucket: str, key: str, file_path: Path, overwrite: bool) -> bool:
    if not overwrite and object_exists(client, bucket, key):
        print(f"[skip] Exists: s3://{bucket}/{key}")
        return False
    client.upload_file(str(file_path), bucket, key)
    print(f"[ok] Uploaded: s3://{bucket}/{key}")
    return True


def download_bytes(url: str, timeout_s: int) -> bytes:
    resp = requests.get(url, timeout=timeout_s)
    resp.raise_for_status()
    return resp.content


def upload_truck_routes(client, args: argparse.Namespace, tmpdir: Path, local_static_dir: Path) -> int:
    uploaded = 0
    shapefile_exts = {".shp", ".shx", ".dbf", ".prj", ".cpg"}
    dataset_files: list[Path] = []

    if args.mode in {"remote", "auto"}:
        try:
            print(f"[info] Downloading truck routes shapefile zip: {TRUCK_ROUTES_SHP_ZIP_URL}")
            zip_bytes = download_bytes(TRUCK_ROUTES_SHP_ZIP_URL, args.timeout)
            zip_path = tmpdir / "truck_routes.zip"
            zip_path.write_bytes(zip_bytes)
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(tmpdir / "truck_routes_extracted")
            dataset_files = [p for p in (tmpdir / "truck_routes_extracted").rglob("*") if p.is_file()]
        except Exception as exc:
            if args.mode == "remote":
                raise
            print(f"[warn] Remote truck route download failed, trying local fallback: {exc}")

    if not dataset_files and args.mode in {"local", "auto"}:
        dataset_files = [p for p in (local_static_dir / "truck_routes").glob("*") if p.is_file()]

    selected = [p for p in dataset_files if p.suffix.lower() in shapefile_exts]
    if not selected:
        raise RuntimeError("No shapefile bundle files found for truck_routes.")

    for file_path in selected:
        key = f"{args.prefix}/truck_routes/{file_path.name}"
        if upload_file_path(client, args.bucket, key, file_path, args.overwrite):
            uploaded += 1
    return uploaded


def upload_congestion_zones(client, args: argparse.Namespace, tmpdir: Path, local_static_dir: Path) -> int:
    uploaded = 0
    source_path: Path | None = None

    if args.mode in {"remote", "auto"}:
        try:
            print(f"[info] Downloading congestion zones geojson: {CONGESTION_ZONES_GEOJSON_URL}")
            data = download_bytes(CONGESTION_ZONES_GEOJSON_URL, args.timeout)
            source_path = tmpdir / "congestion_zones.geojson"
            source_path.write_bytes(data)
        except Exception as exc:
            if args.mode == "remote":
                raise
            print(f"[warn] Remote congestion download failed, trying local fallback: {exc}")

    if source_path is None and args.mode in {"local", "auto"}:
        local_files = list((local_static_dir / "congestion_zones").glob("*.geojson"))
        source_path = local_files[0] if local_files else None

    if source_path is None:
        raise RuntimeError("No congestion zone GeoJSON file found.")

    key = f"{args.prefix}/congestion_zones/{source_path.name}"
    if upload_file_path(client, args.bucket, key, source_path, args.overwrite):
        uploaded += 1
    return uploaded


def upload_building_energy(client, args: argparse.Namespace, tmpdir: Path, local_static_dir: Path) -> int:
    uploaded = 0
    source_path: Path | None = None

    if args.mode in {"remote", "auto"}:
        try:
            print(f"[info] Downloading LL84 CSV: {LL84_CSV_URL}")
            data = download_bytes(LL84_CSV_URL, args.timeout)
            source_path = tmpdir / "ll84_building_energy.csv"
            source_path.write_bytes(data)
        except Exception as exc:
            if args.mode == "remote":
                raise
            print(f"[warn] Remote building energy download failed, trying local fallback: {exc}")

    if source_path is None and args.mode in {"local", "auto"}:
        local_files = list((local_static_dir / "building_energy").glob("*.csv"))
        source_path = local_files[0] if local_files else None

    if source_path is None:
        raise RuntimeError("No LL84 building energy CSV file found.")

    key = f"{args.prefix}/building_energy/{source_path.name}"
    if upload_file_path(client, args.bucket, key, source_path, args.overwrite):
        uploaded += 1
    return uploaded


def main() -> int:
    args = parse_args()
    client = s3_client(args)
    local_static_dir = Path(args.local_static_dir)

    try:
        client.head_bucket(Bucket=args.bucket)
    except ClientError as exc:
        print(f"[error] Cannot access bucket '{args.bucket}': {exc}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory(prefix="static_data_init_") as td:
        tmpdir = Path(td)
        uploaded = 0
        uploaded += upload_truck_routes(client, args, tmpdir, local_static_dir)
        uploaded += upload_congestion_zones(client, args, tmpdir, local_static_dir)
        uploaded += upload_building_energy(client, args, tmpdir, local_static_dir)

    print(f"[ok] Static data onboarding completed. New objects uploaded: {uploaded}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
