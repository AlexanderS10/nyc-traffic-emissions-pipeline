#!/usr/bin/env python3
"""
Milestone 3 smoke test for AQ normalization contract.

Validates:
1) OpenAQ and PurpleAir producer payloads include normalized fields:
   { sensor_id, source, lat, lon, pm25, timestamp }
2) Invalid source records are skipped gracefully (no producer crash).

Run in container:
  docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E2_m3_aq_normalization_smoke.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import openaq_producer
import purpleair_producer


def assert_normalized_contract(payload: dict, expected_source: str) -> None:
    required = {"sensor_id", "source", "lat", "lon", "pm25", "timestamp"}
    missing = required - set(payload.keys())
    assert not missing, f"Missing normalized keys: {missing}"
    assert payload["source"] == expected_source
    assert isinstance(payload["sensor_id"], str)
    assert isinstance(payload["lat"], float)
    assert isinstance(payload["lon"], float)
    assert isinstance(payload["pm25"], float)
    assert isinstance(payload["timestamp"], str) and payload["timestamp"]


def check_openaq() -> None:
    captured_values: list[dict] = []

    class FakeProducer:
        def produce(self, *args, **kwargs):
            captured_values.append(json.loads(kwargs["value"].decode("utf-8")))

        def flush(self, *args, **kwargs):
            return None

    def fake_get_json(logger, url, **kwargs):
        if "locations" in url:
            return {
                "results": [
                    {
                        "id": 1,
                        "name": "valid-location",
                        "coordinates": {"latitude": 40.71, "longitude": -74.0},
                        "sensors": [{"id": 101, "parameter": {"name": "pm25"}}],
                    },
                    {
                        "id": 2,
                        "name": "invalid-location",
                        "coordinates": {"latitude": None, "longitude": -74.0},
                        "sensors": [{"id": 102, "parameter": {"name": "pm25"}}],
                    },
                ]
            }
        return {"results": [{"value": 8.4, "period": {"datetimeTo": {"utc": "2026-05-03T20:00:00Z"}}}]}

    with patch.object(openaq_producer, "producer", FakeProducer()), patch.object(
        openaq_producer, "get_json_with_retry", side_effect=fake_get_json
    ), patch.object(openaq_producer.time, "sleep", return_value=None):
        openaq_producer.fetch_and_send()

    assert len(captured_values) == 1, f"Expected 1 valid OpenAQ payload, got {len(captured_values)}"
    assert_normalized_contract(captured_values[0], "openaq")


def check_purpleair() -> None:
    captured_values: list[dict] = []

    class FakeProducer:
        def produce(self, *args, **kwargs):
            captured_values.append(json.loads(kwargs["value"].decode("utf-8")))

        def flush(self, *args, **kwargs):
            return None

    purpleair_payload = {
        "fields": ["sensor_index", "name", "latitude", "longitude", "pm2.5_10minute", "temperature", "humidity"],
        "data": [
            [3001, "valid-sensor", 40.72, -73.98, 5.1, 70, 45],
            [3002, "invalid-sensor", 40.73, -73.97, None, 71, 44],
        ],
    }

    with patch.object(purpleair_producer, "producer", FakeProducer()), patch.object(
        purpleair_producer, "get_json_with_retry", return_value=purpleair_payload
    ):
        purpleair_producer.fetch_and_send()

    assert len(captured_values) == 1, f"Expected 1 valid PurpleAir payload, got {len(captured_values)}"
    assert_normalized_contract(captured_values[0], "purpleair")


def main() -> int:
    check_openaq()
    print("[ok] OpenAQ mapping emits normalized payload and skips invalid records.")

    check_purpleair()
    print("[ok] PurpleAir mapping emits normalized payload and skips invalid records.")

    print("[ok] Milestone 3 AQ normalization smoke test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
