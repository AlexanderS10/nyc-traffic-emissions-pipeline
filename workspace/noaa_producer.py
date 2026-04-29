import requests
import json
import time
import time
import os
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()
CONTACT_EMAIL = os.getenv("NOAA_CONTACT_EMAIL", "your_email@example.com")
KAFKA_BROKER = "redpanda:29092"
TOPIC_NAME = "nyc_weather_raw"
POLL_INTERVAL = 3600

HEADERS = {
    "User-Agent": f"NYCAirQualityProject ({CONTACT_EMAIL})",
    "Accept": "application/geo+json"
}

# Geographic centroids for each NYC borough
NYC_BOROUGHS = {
    "manhattan":     {"lat": 40.7831, "lon": -73.9712},
    "brooklyn":      {"lat": 40.6782, "lon": -73.9442},
    "queens":        {"lat": 40.7282, "lon": -73.7949},
    "bronx":         {"lat": 40.8448, "lon": -73.8648},
    "staten_island": {"lat": 40.5795, "lon": -74.1502},
}

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "nyc-weather-producer"
})

# In-memory cache: borough -> resolved forecastHourly URL
# Avoids re-calling /points/ on every poll cycle
_forecast_url_cache: dict[str, str] = {}


def delivery_report(err, msg):
    if err is not None:
        print(f"[kafka] Delivery failed for {msg.key()}: {err}")


def resolve_forecast_url(borough: str, lat: float, lon: float) -> str | None:
    """
    Calls /points/{lat},{lon} to dynamically resolve the correct forecastHourly URL.
    This is the only correct way to get NOAA grid coordinates — never hardcode them.
    """
    if borough in _forecast_url_cache:
        return _forecast_url_cache[borough]

    url = f"https://api.weather.gov/points/{lat},{lon}"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        props = resp.json().get("properties", {})
        forecast_hourly_url = props.get("forecastHourly")

        if not forecast_hourly_url:
            print(f"[{borough}] ERROR: No forecastHourly in /points/ response.")
            return None

        grid_id = props.get("gridId")
        grid_x  = props.get("gridX")
        grid_y  = props.get("gridY")
        print(f"[{borough}] Resolved grid: {grid_id}/{grid_x},{grid_y}")

        _forecast_url_cache[borough] = forecast_hourly_url
        return forecast_hourly_url

    except requests.exceptions.RequestException as e:
        print(f"[{borough}] Failed to resolve /points/: {e}")
        return None


def fetch_and_send_borough(borough: str, lat: float, lon: float):
    forecast_url = resolve_forecast_url(borough, lat, lon)
    if not forecast_url:
        return

    try:
        resp = requests.get(forecast_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        periods = resp.json().get("properties", {}).get("periods", [])

        if not periods:
            print(f"[{borough}] No forecast periods returned.")
            return

        count = 0
        for period in periods[:5]:  # next 5 hours is sufficient for a 15-min window join
            payload = {
                **period,
                "borough": borough,
                "source_lat": lat,
                "source_lon": lon,
            }
            # Composite key keeps same borough on the same Kafka partition
            kafka_key = f"{borough}::{period.get('startTime', 'unknown')}"

            producer.produce(
                topic=TOPIC_NAME,
                key=kafka_key.encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
                callback=delivery_report,
            )
            count += 1

        producer.flush()
        print(f"[{borough}] Pushed {count} hourly forecasts to '{TOPIC_NAME}'.")

    except requests.exceptions.RequestException as e:
        print(f"[{borough}] Forecast fetch error: {e}")
        # If NOAA rotated the grid cell, bust the cache so we re-resolve next poll
        if "404" in str(e):
            _forecast_url_cache.pop(borough, None)
            print(f"[{borough}] Cache busted — will re-resolve grid on next poll.")
    except Exception as e:
        print(f"[{borough}] Unexpected error: {e}")


def fetch_all_boroughs():
    print("--- Polling NOAA weather for all NYC boroughs ---")
    for borough, coords in NYC_BOROUGHS.items():
        fetch_and_send_borough(borough, coords["lat"], coords["lon"])
        time.sleep(1)  # rate-limit courtesy between borough calls


if __name__ == "__main__":
    print(f"Starting NYC Weather Producer. Polling every {POLL_INTERVAL}s...")
    while True:
        fetch_all_boroughs()
        time.sleep(POLL_INTERVAL)