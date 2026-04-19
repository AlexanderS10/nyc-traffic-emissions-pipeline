import requests
import json
import time
import os
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")
KAFKA_BROKER   = "redpanda:29092"
TOPIC_NAME     = "nyc_openaq_raw"
POLL_INTERVAL  = 300  # 5 minutes

LOCATIONS_URL    = "https://api.openaq.org/v3/locations"
MEASUREMENTS_URL = "https://api.openaq.org/v3/sensors/{sensor_id}/measurements"

# 25km radius around NYC centre — catches all 5 boroughs
LOCATION_PARAMS = {
    "coordinates": "40.7128,-74.0060",
    "radius": 25000,
    "limit": 50,
}

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "nyc-openaq-producer"
})


def delivery_report(err, msg):
    if err is not None:
        print(f"[kafka] Delivery failed for {msg.key()}: {err}")


def fetch_and_send():
    if not OPENAQ_API_KEY:
        print("ERROR: OPENAQ_API_KEY not found in .env")
        return

    headers = {
        "X-API-Key": OPENAQ_API_KEY,
        "Accept":    "application/json"
    }

    # ── Step 1: Discover NYC sensor locations ─────────────────────────────────
    try:
        print("Fetching OpenAQ locations for NYC...")
        loc_resp = requests.get(LOCATIONS_URL, headers=headers,
                                params=LOCATION_PARAMS, timeout=30)
        loc_resp.raise_for_status()
        locations = loc_resp.json().get("results", [])
    except requests.exceptions.RequestException as e:
        print(f"Locations fetch error: {e}")
        return

    if not locations:
        print("No locations returned.")
        return

    pushed = 0

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
                meas_resp = requests.get(
                    MEASUREMENTS_URL.format(sensor_id=sensor_id),
                    headers=headers,
                    params={"limit": 1},  # only the most recent reading
                    timeout=30
                )
                meas_resp.raise_for_status()
                readings = meas_resp.json().get("results", [])
            except requests.exceptions.RequestException as e:
                print(f"  Sensor {sensor_id} error: {e}")
                continue

            if not readings:
                continue

            reading = readings[0]

            # Build a flat, schema-friendly payload for Spark
            payload = {
                "sensor_id":     sensor_id,
                "location_id":   location_id,
                "location_name": location_name,
                "latitude":      coordinates.get("latitude"),
                "longitude":     coordinates.get("longitude"),
                "parameter":     param_name,
                "value":         reading.get("value"),          # the PM2.5 μg/m³ reading
                "unit":          reading.get("parameter", {}).get("units"),
                "datetime_utc":  reading.get("period", {}).get("datetimeTo", {}).get("utc"),
            }

            producer.produce(
                topic=TOPIC_NAME,
                key=str(sensor_id).encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
                callback=delivery_report,
            )
            pushed += 1

        # Small courtesy delay between sensor calls to respect rate limits
        time.sleep(0.2)

    producer.flush()
    print(f"Pushed {pushed} PM2.5 readings to '{TOPIC_NAME}'.")


if __name__ == "__main__":
    print(f"Starting NYC OpenAQ Producer. Polling every {POLL_INTERVAL}s...")
    while True:
        fetch_and_send()
        time.sleep(POLL_INTERVAL)