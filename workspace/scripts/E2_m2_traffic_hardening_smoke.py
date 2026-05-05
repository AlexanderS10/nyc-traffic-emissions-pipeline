#!/usr/bin/env python3
"""
Milestone 2 smoke test for traffic producer hardening.

Validates:
1) Retry/backoff for retryable HTTP statuses.
2) Message key uses current fetch timestamp (not sensor id).
3) Transient request failure does not crash fetch loop behavior.

Run in container:
  docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E2_m2_traffic_hardening_smoke.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import producer_common
import traffic_producer


class FakeResponse:
    def __init__(self, status_code: int, payload):
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self):
        if self.status_code >= 400:
            exc = requests.exceptions.HTTPError(f"status={self.status_code}")
            exc.response = self
            raise exc

    def json(self):
        return self._payload


def check_retry_backoff() -> None:
    call_count = {"n": 0}

    def fake_get(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return FakeResponse(429, {})
        return FakeResponse(200, {"ok": True})

    with patch("producer_common.requests.get", side_effect=fake_get), patch("producer_common.time.sleep", return_value=None):
        result = producer_common.get_json_with_retry(
            producer_common.get_logger("m2_traffic_hardening_smoke"),
            "http://example.com/fake",
            max_retries=2,
            backoff_base_s=0.01,
        )

    assert result == {"ok": True}
    assert call_count["n"] == 2, f"Expected 2 calls, got {call_count['n']}"


def check_timestamp_keying() -> None:
    captured = {"key": None}

    class FakeProducer:
        def produce(self, *args, **kwargs):
            captured["key"] = kwargs.get("key")

        def flush(self, *args, **kwargs):
            return None

    sample_records = [{"id": "123", "data_as_of": "2026-05-03T20:00:00.000"}]
    fake_producer = FakeProducer()

    with patch.object(traffic_producer, "get_json_with_retry", return_value=sample_records), patch.object(
        traffic_producer, "producer", fake_producer
    ):
        newest = traffic_producer.fetch_and_send("")

    assert newest == "2026-05-03T20:00:00.000"
    key = (captured["key"] or b"").decode("utf-8")
    assert key.endswith("Z"), f"Expected UTC timestamp key, got: {key}"
    assert key != "123", "Traffic key should be fetch timestamp, not sensor id"


def check_transient_recovery() -> None:
    with patch.object(
        traffic_producer,
        "get_json_with_retry",
        side_effect=requests.exceptions.RequestException("simulated transient failure"),
    ):
        out = traffic_producer.fetch_and_send("2026-05-03T20:10:00")
        assert out == "2026-05-03T20:10:00"


def main() -> int:
    check_retry_backoff()
    print("[ok] Retry/backoff behavior validated for retryable status.")

    check_timestamp_keying()
    print("[ok] Traffic producer uses fetch timestamp as Kafka key.")

    check_transient_recovery()
    print("[ok] Transient request failure handling returns safely without crash.")

    print("[ok] Milestone 2 traffic hardening smoke test passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
