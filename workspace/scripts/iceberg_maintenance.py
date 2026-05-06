#!/usr/bin/env python3
"""
Iceberg table maintenance: compaction + snapshot expiry.

Runs rewrite_data_files (optimize) and expire_snapshots against
local.db.enriched_traffic via the Trino HTTP API.

Usage (inside jupyter-pyspark container):
  # one-shot
  python iceberg_maintenance.py

  # scheduled every 24 h
  python iceberg_maintenance.py --schedule

Run from host:
  docker exec -it jupyter-pyspark python /home/jovyan/work/scripts/iceberg_maintenance.py
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone

import requests

# ── config ────────────────────────────────────────────────────────────────────
TRINO_HOST      = "http://trino:8080"   # inside Docker network
TABLE           = "iceberg.db.enriched_traffic"
TARGET_FILE_MB  = 128                   # compact files smaller than this
SNAPSHOT_TTL    = "7d"                  # expire snapshots older than this
SCHEDULE_HRS    = 24
# ──────────────────────────────────────────────────────────────────────────────


def _poll(first: dict, headers: dict) -> list:
    """Follow Trino nextUri chain and collect all data rows."""
    rows = []
    result = first
    while True:
        rows.extend(result.get("data") or [])
        next_uri = result.get("nextUri")
        if not next_uri:
            state = result.get("stats", {}).get("state", "UNKNOWN")
            if state not in ("FINISHED", "FAILED"):
                # shouldn't happen, but guard against partial responses
                time.sleep(0.3)
                result = requests.get(next_uri or "", headers=headers, timeout=30).json()
                continue
            break
        time.sleep(0.3)
        result = requests.get(next_uri, headers=headers, timeout=30).json()
    return rows


def run(query: str) -> list:
    headers = {"X-Trino-User": "admin", "X-Trino-Source": "iceberg-maintenance"}
    resp = requests.post(
        f"{TRINO_HOST}/v1/statement",
        data=query,
        headers=headers,
        timeout=30,
    )
    resp.raise_for_status()
    result = resp.json()

    # surface errors early
    if result.get("error"):
        raise RuntimeError(result["error"].get("message", str(result["error"])))

    return _poll(result, headers)


def file_count() -> int:
    rows = run(f'SELECT COUNT(*) FROM "{TABLE.split(".")[0]}"."{TABLE.split(".")[1]}"."{TABLE.split(".")[2]}$files"')
    return int(rows[0][0]) if rows else 0


def snapshot_count() -> int:
    rows = run(f'SELECT COUNT(*) FROM "{TABLE.split(".")[0]}"."{TABLE.split(".")[1]}"."{TABLE.split(".")[2]}$snapshots"')
    return int(rows[0][0]) if rows else 0


def row_count() -> int:
    rows = run(f"SELECT COUNT(*) FROM {TABLE}")
    return int(rows[0][0]) if rows else 0


def maintenance_run() -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"\n[{ts}] Starting Iceberg maintenance on {TABLE}")

    before_files     = file_count()
    before_snapshots = snapshot_count()
    before_rows      = row_count()
    print(f"  Before → files={before_files}  snapshots={before_snapshots}  rows={before_rows}")

    print("  Step 1/3 — optimize (rewrite_data_files) ...")
    run(f"ALTER TABLE {TABLE} EXECUTE optimize(file_size_threshold => '{TARGET_FILE_MB}MB')")
    print(f"           done  files now={file_count()}")

    print("  Step 2/3 — expire_snapshots ...")
    run(f"ALTER TABLE {TABLE} EXECUTE expire_snapshots(retention_threshold => '{SNAPSHOT_TTL}')")
    print(f"           done  snapshots now={snapshot_count()}")

    print("  Step 3/3 — remove_orphan_files ...")
    run(f"ALTER TABLE {TABLE} EXECUTE remove_orphan_files(retention_threshold => '{SNAPSHOT_TTL}')")
    print("           done")

    after_files     = file_count()
    after_snapshots = snapshot_count()
    after_rows      = row_count()
    print(
        f"  After  → files={after_files}  snapshots={after_snapshots}  rows={after_rows}\n"
        f"  Compacted {before_files - after_files} file(s), "
        f"expired {before_snapshots - after_snapshots} snapshot(s). "
        f"Row count unchanged: {after_rows == before_rows}"
    )
    if after_rows != before_rows:
        print(f"  WARNING: row count changed {before_rows} → {after_rows}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="Iceberg table maintenance")
    parser.add_argument(
        "--schedule",
        action="store_true",
        help=f"Run in a loop every {SCHEDULE_HRS} hours (default: run once and exit)",
    )
    args = parser.parse_args()

    if args.schedule:
        print(f"Scheduled mode: running every {SCHEDULE_HRS} hours. Ctrl-C to stop.")
        while True:
            maintenance_run()
            next_run = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ",
                time.gmtime(time.time() + SCHEDULE_HRS * 3600),
            )
            print(f"  Next run at {next_run} UTC. Sleeping ...")
            time.sleep(SCHEDULE_HRS * 3600)
    else:
        maintenance_run()


if __name__ == "__main__":
    main()
