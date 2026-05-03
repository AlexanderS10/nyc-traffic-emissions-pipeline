import requests
import json
import time
from datetime import datetime, timezone
from producer_common import TOPICS, create_producer, get_json_with_retry, get_logger, get_required_env, log_startup

OPENAQ_API_KEY = get_required_env("OPENAQ_API_KEY")
TOPIC_NAME = TOPICS["openaq"]
POLL_INTERVAL  = 300  # 5 minutes

LOCATIONS_URL    = "https://api.openaq.org/v3/locations"
MEASUREMENTS_URL = "https://api.openaq.org/v3/sensors/{sensor_id}/measurements"

# 25km radius around NYC centre — catches all 5 boroughs
LOCATION_PARAMS = {
    "coordinates": "40.7128,-74.0060",
    "radius": 25000,
    "limit": 50,
}

logger = get_logger("openaq_producer")
producer = create_producer("nyc-openaq-producer")


def to_iso_utc(ts_value: str | None) -> str | None:
    if not ts_value:
        return None
    try:
        if ts_value.endswith("Z"):
            return ts_value
        dt = datetime.fromisoformat(ts_value.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return None


def to_float(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def delivery_report(err, msg):
    if err is not None:
        logger.error("kafka_delivery_failed key=%s error=%s", msg.key(), err)


def fetch_and_send():
    headers = {
        "X-API-Key": OPENAQ_API_KEY,
        "Accept":    "application/json"
    }

    # ── Step 1: Discover NYC sensor locations ─────────────────────────────────
    try:
        logger.info("fetch_openaq_locations")
        location_payload = get_json_with_retry(
            logger,
            LOCATIONS_URL,
            headers=headers,
            params=LOCATION_PARAMS,
        )
        locations = location_payload.get("results", [])
    except requests.exceptions.RequestException as e:
        logger.error("locations_fetch_error error=%s", e)
        return

    if not locations:
        logger.info("no_locations_returned")
        return

    pushed = 0
    skipped = 0

    # ── Step 2: For each location pull its latest PM2.5 reading ───────────────
    for location in locations:
        location_id   = location.get("id")
        location_name = location.get("name", "unknown")
        coordinates   = location.get("coordinates", {})

        for sensor in location.get("sensors", []):
            param = sensor.get("parameter", {})

            # param can be a dict (v3) or a string (older responses) — handle both
            param_name = (
                param.get("name") if isinstance(param, dict) else str(param)
            )

            # Only pull PM2.5 to stay within rate limits
            if param_name != "pm25":
                continue

            sensor_id = sensor.get("id")
            if not sensor_id:
                continue

            try:
                measurements_payload = get_json_with_retry(
                    logger,
                    MEASUREMENTS_URL.format(sensor_id=sensor_id),
                    headers=headers,
                    params={"limit": 1},
                )
                readings = measurements_payload.get("results", [])
            except requests.exceptions.RequestException as e:
                logger.warning("sensor_measurement_fetch_error sensor_id=%s error=%s", sensor_id, e)
                continue

            if not readings:
                continue

            reading = readings[0]
            pm25 = to_float(reading.get("value"))
            lat = to_float(coordinates.get("latitude"))
            lon = to_float(coordinates.get("longitude"))
            timestamp = to_iso_utc(reading.get("period", {}).get("datetimeTo", {}).get("utc"))

            # Enforce normalized AQ contract required by Milestone 3.
            if pm25 is None or lat is None or lon is None or not timestamp:
                skipped += 1
                logger.warning(
                    "skip_invalid_normalized_record source=openaq sensor_id=%s lat=%s lon=%s pm25=%s timestamp=%s",
                    sensor_id,
                    lat,
                    lon,
                    pm25,
                    timestamp,
                )
                continue

            # Normalized contract fields:
            # { sensor_id, source, lat, lon, pm25, timestamp }
            # Keep legacy compatibility fields for existing notebook schema.
            payload = {
                "sensor_id":     str(sensor_id),
                "source":        "openaq",
                "lat":           lat,
                "lon":           lon,
                "pm25":          pm25,
                "timestamp":     timestamp,
                "location_id":   location_id,
                "location_name": location_name,
                "latitude":      lat,
                "longitude":     lon,
                "parameter":     param_name,
                "value":         pm25,
                "unit":          reading.get("parameter", {}).get("units"),
                "datetime_utc":  timestamp,
            }

            producer.produce(
                topic=TOPIC_NAME,
                key=f"openaq::{sensor_id}".encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
                callback=delivery_report,
            )
            pushed += 1

        # Small courtesy delay between sensor calls to respect rate limits
        time.sleep(0.2)

    producer.flush()
    logger.info("kafka_batch_sent topic=%s records=%s skipped=%s", TOPIC_NAME, pushed, skipped)


if __name__ == "__main__":
    log_startup(logger, "nyc-openaq-producer", TOPIC_NAME, POLL_INTERVAL)
    while True:
        fetch_and_send()
        time.sleep(POLL_INTERVAL)