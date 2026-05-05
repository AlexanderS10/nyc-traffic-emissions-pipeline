#!/usr/bin/env python3
"""Milestone 8 runtime import smoke check."""

from __future__ import annotations

import importlib
import sys


MODULES = (
    ("pyspark", "pyspark"),
    ("h3", "h3"),
    ("apache-sedona", "sedona"),
)


def main() -> int:
    for label, module_name in MODULES:
        importlib.import_module(module_name)
        print(f"[ok] Imported {label} ({module_name})")

    print("[ok] Runtime import smoke check passed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # pragma: no cover
        print(f"[error] {exc}", file=sys.stderr)
        raise SystemExit(1)
