from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Any

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv


# Ensure all producer scripts read workspace/.env consistently.
load_dotenv(dotenv_path=Path(__file__).with_name(".env"))

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "redpanda:29092")
DEFAULT_REQUEST_TIMEOUT_S = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))
DEFAULT_MAX_RETRIES = int(os.getenv("HTTP_MAX_RETRIES", "3"))
DEFAULT_BACKOFF_BASE_S = float(os.getenv("HTTP_BACKOFF_BASE_SECONDS", "1.5"))

TOPICS = {
    "traffic": "nyc_traffic_raw",
    "openaq": "nyc_openaq_raw",
    "purpleair": "nyc_purpleair_raw",
    "weather": "nyc_weather_raw",
}

RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def get_logger(name: str) -> logging.Logger:
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s | %(message)s",
        )
    return logging.getLogger(name)


def get_required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} not found in workspace/.env — aborting.")
    return value


def create_producer(client_id: str) -> Producer:
    return Producer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "client.id": client_id,
        }
    )


def log_startup(logger: logging.Logger, producer_name: str, topic: str, poll_interval: int) -> None:
    logger.info(
        "startup producer=%s topic=%s broker=%s poll_interval_seconds=%s timeout_seconds=%s max_retries=%s",
        producer_name,
        topic,
        KAFKA_BROKER,
        poll_interval,
        DEFAULT_REQUEST_TIMEOUT_S,
        DEFAULT_MAX_RETRIES,
    )


def get_json_with_retry(
    logger: logging.Logger,
    url: str,
    *,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    timeout: int = DEFAULT_REQUEST_TIMEOUT_S,
    max_retries: int = DEFAULT_MAX_RETRIES,
    backoff_base_s: float = DEFAULT_BACKOFF_BASE_S,
) -> dict[str, Any] | list[Any]:
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
            if response.status_code in RETRYABLE_STATUS_CODES:
                raise requests.exceptions.HTTPError(
                    f"retryable_status={response.status_code}",
                    response=response,
                )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            if attempt >= max_retries:
                logger.error("request_failed url=%s error=%s", url, exc)
                raise

            sleep_s = backoff_base_s * (2**attempt)
            logger.warning(
                "request_retry url=%s attempt=%s/%s sleep_seconds=%.1f error=%s",
                url,
                attempt + 1,
                max_retries + 1,
                sleep_s,
                exc,
            )
            time.sleep(sleep_s)

    raise RuntimeError("Unexpected retry flow termination")
