import requests
import json
import time
from datetime import datetime, timezone
from producer_common import TOPICS, create_producer, get_json_with_retry, get_logger, get_required_env, log_startup

PURPLEAIR_API_KEY = get_required_env("PURPLEAIR_API_KEY")
TOPIC_NAME = TOPICS["purpleair"]
POLL_INTERVAL = 120  # 2 minutes (PurpleAir updates relatively frequently)

# PurpleAir v1 API endpoint
API_URL = "https://api.purpleair.com/v1/sensors"

# Bounding box roughly covering New York City (NW to SE)
PARAMS = {
    "fields": "sensor_index,name,latitude,longitude,pm2.5_10minute,temperature,humidity",
    "nwlng": "-74.2590",
    "nwlat": "40.9176",
    "selng": "-73.7003",
    "selat": "40.4774"
}

logger = get_logger("purpleair_producer")
producer = create_producer("nyc-purpleair-producer")


def to_float(value) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def now_iso_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def delivery_report(err, msg):
    if err is not None:
        logger.error("kafka_delivery_failed key=%s error=%s", msg.key(), err)

def fetch_and_send():
    """
    Fetches the latest air quality readings for NYC sensors and pushes them to Kafka.
    """
    headers = {
        "X-API-Key": PURPLEAIR_API_KEY
    }

    try:
        logger.info("fetch_purpleair")
        data = get_json_with_retry(
            logger,
            API_URL,
            headers=headers,
            params=PARAMS,
        )
        
        # PurpleAir returns a "data" array of arrays, and a "fields" array explaining the columns
        fields = data.get("fields", [])
        sensor_data = data.get("data", [])

        if not sensor_data:
            logger.info("no_sensor_data")
            return

        skipped = 0

        # Convert row arrays into normalized JSON objects.
        for row in sensor_data:
            record = dict(zip(fields, row))
            sensor_id = record.get("sensor_index")
            lat = to_float(record.get("latitude"))
            lon = to_float(record.get("longitude"))
            pm25 = to_float(record.get("pm2.5_10minute"))
            timestamp = now_iso_utc()

            # Enforce normalized AQ contract required by Milestone 3.
            if sensor_id is None or lat is None or lon is None or pm25 is None:
                skipped += 1
                logger.warning(
                    "skip_invalid_normalized_record source=purpleair sensor_id=%s lat=%s lon=%s pm25=%s",
                    sensor_id,
                    lat,
                    lon,
                    pm25,
                )
                continue

            # Normalized contract fields:
            # { sensor_id, source, lat, lon, pm25, timestamp }
            # Keep legacy compatibility fields for existing notebook schema.
            payload_obj = {
                "sensor_id": str(sensor_id),
                "source": "purpleair",
                "lat": lat,
                "lon": lon,
                "pm25": pm25,
                "timestamp": timestamp,
                "sensor_index": sensor_id,
                "name": record.get("name"),
                "latitude": lat,
                "longitude": lon,
                "pm2.5_10minute": pm25,
                "temperature": record.get("temperature"),
                "humidity": record.get("humidity"),
            }

            producer.produce(
                topic=TOPIC_NAME,
                key=f"purpleair::{sensor_id}".encode("utf-8"),
                value=json.dumps(payload_obj).encode("utf-8"),
                callback=delivery_report,
            )

        producer.flush()
        logger.info("kafka_batch_sent topic=%s records=%s skipped=%s", TOPIC_NAME, len(sensor_data) - skipped, skipped)

    except requests.exceptions.RequestException as e:
        logger.error("api_error error=%s", e)
    except Exception as e:
        logger.error("unexpected_error error=%s", e)

if __name__ == "__main__":
    log_startup(logger, "nyc-purpleair-producer", TOPIC_NAME, POLL_INTERVAL)
    while True:
        fetch_and_send()
        time.sleep(POLL_INTERVAL)