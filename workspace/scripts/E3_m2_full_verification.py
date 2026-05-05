#!/usr/bin/env python3
"""
Run Milestone 2 verification checks with one command.

Default flow:
1) Console parsing probe for nyc_traffic_raw
2) Console parsing probe for nyc_openaq_raw
3) Inject one malformed JSON message
4) Verify malformed record landed in quarantine
5) Run local JSON parsing smoke assertions

Run in container:
  docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E3_m2_full_verification.py
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import Sequence


SCRIPTS_DIR = Path(__file__).resolve().parent
CONSOLE_PROBE = SCRIPTS_DIR / "E3_m2_console_parsing_probe.py"
INJECT_MALFORMED = SCRIPTS_DIR / "E3_m2_inject_malformed_payload.py"
VERIFY_QUARANTINE = SCRIPTS_DIR / "E3_m2_verify_quarantine_record.py"
JSON_SMOKE = SCRIPTS_DIR / "E3_m2_json_parsing_smoke.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="One-command Milestone 2 verification wrapper")
    parser.add_argument(
        "--probe-topics",
        nargs="+",
        default=["nyc_traffic_raw", "nyc_openaq_raw"],
        help="Kafka topics to run console parsing probes against",
    )
    parser.add_argument(
        "--probe-duration-seconds",
        type=int,
        default=35,
        help="Duration per console probe run",
    )
    parser.add_argument(
        "--probe-master",
        default="local[2]",
        help="Spark master for console probe script",
    )
    parser.add_argument(
        "--bootstrap",
        default="redpanda:29092",
        help="Kafka bootstrap server passed to probe/inject scripts",
    )
    parser.add_argument(
        "--inject-topic",
        default="nyc_purpleair_raw",
        help="Topic where malformed payload will be injected",
    )
    parser.add_argument(
        "--skip-inject-verify",
        action="store_true",
        help="Skip malformed injection + quarantine verification steps",
    )
    parser.add_argument(
        "--skip-json-smoke",
        action="store_true",
        help="Skip local JSON parsing smoke script",
    )
    parser.add_argument(
        "--verify-retries",
        type=int,
        default=4,
        help="Retries for quarantine verify step",
    )
    parser.add_argument(
        "--verify-wait-seconds",
        type=int,
        default=8,
        help="Wait between quarantine verify retries",
    )
    return parser.parse_args()


def run_step(step_name: str, command: Sequence[str]) -> None:
    printable = " ".join(command)
    print(f"\n[step] {step_name}")
    print(f"[cmd ] {printable}")
    completed = subprocess.run(command, check=False)
    if completed.returncode != 0:
        print(f"[fail] {step_name} (exit_code={completed.returncode})")
        raise SystemExit(completed.returncode)
    print(f"[ok  ] {step_name}")


def run_step_with_result(step_name: str, command: Sequence[str]) -> int:
    printable = " ".join(command)
    print(f"\n[step] {step_name}")
    print(f"[cmd ] {printable}")
    completed = subprocess.run(command, check=False)
    if completed.returncode == 0:
        print(f"[ok  ] {step_name}")
    else:
        print(f"[warn] {step_name} failed (exit_code={completed.returncode})")
    return completed.returncode


def ensure_scripts_exist() -> None:
    required = [CONSOLE_PROBE, INJECT_MALFORMED, VERIFY_QUARANTINE, JSON_SMOKE]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        print("[error] Required script(s) not found:")
        for path in missing:
            print(f"  - {path}")
        raise SystemExit(1)


def main() -> int:
    args = parse_args()
    ensure_scripts_exist()

    py = sys.executable

    print(
        "[info] Prereq for malformed verification: keep the pipeline notebook "
        "running so malformed_json_query writes to s3a://raw-data/quarantine/malformed_json/"
    )

    for topic in args.probe_topics:
        run_step(
            f"Console parsing probe ({topic})",
            [
                py,
                str(CONSOLE_PROBE),
                "--topic",
                topic,
                "--master",
                args.probe_master,
                "--bootstrap",
                args.bootstrap,
                "--duration-seconds",
                str(args.probe_duration_seconds),
            ],
        )

    if not args.skip_inject_verify:
        run_step(
            "Inject malformed payload",
            [
                py,
                str(INJECT_MALFORMED),
                "--bootstrap",
                args.bootstrap,
                "--topic",
                args.inject_topic,
            ],
        )
        verify_attempts = max(1, args.verify_retries)
        verify_rc = 1
        for attempt in range(1, verify_attempts + 1):
            verify_rc = run_step_with_result(
                f"Verify malformed quarantine record (attempt {attempt}/{verify_attempts})",
                [py, str(VERIFY_QUARANTINE)],
            )
            if verify_rc == 0:
                break
            if attempt < verify_attempts:
                print(f"[info] Waiting {args.verify_wait_seconds}s before retry...")
                time.sleep(args.verify_wait_seconds)

        if verify_rc != 0:
            print(
                "[error] Quarantine verification did not pass. "
                "Most likely the streaming notebook is not running or has not yet processed the injected record."
            )
            return verify_rc
    else:
        print("\n[skip] Malformed inject/verify steps skipped by flag.")

    if not args.skip_json_smoke:
        run_step(
            "Local JSON parsing smoke",
            [py, str(JSON_SMOKE)],
        )
    else:
        print("\n[skip] JSON parsing smoke step skipped by flag.")

    print("\n[ok] Milestone 2 full verification completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
