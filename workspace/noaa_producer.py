import requests
import json
import time
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load API keys from the .env file
load_dotenv()
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

if not OPENWEATHER_API_KEY:
    raise RuntimeError("OPENWEATHER_API_KEY not found in .env — aborting.")

KAFKA_BROKER = "redpanda:29092"
TOPIC_NAME = "nyc_weather_raw"
POLL_INTERVAL = 300  # 5 minutes! This gives us near real-time weather ingestion.

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

def delivery_report(err, msg):
    if err is not None:
        print(f"[kafka] Delivery failed for {msg.key()}: {err}")

def deg_to_compass(num):
    """Converts wind direction in degrees to a compass direction (NOAA style)."""
    val = int((num / 22.5) + .5)
    arr = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return arr[(val % 16)]

def fetch_and_send_borough(borough: str, lat: float, lon: float):
    # Call the OpenWeatherMap Current Weather endpoint
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=imperial"
    
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Generate a proper ISO-8601 timestamp for PySpark's to_timestamp() function
        event_time = datetime.fromtimestamp(data["dt"], timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')

        # Translate OpenWeatherMap JSON into the exact schema PySpark expects (NOAA format)
        payload = {
            "name": borough, 
            "startTime": event_time,
            "endTime": event_time, # Current weather, so start and end are the same
            "isDaytime": True, 
            "temperature": int(data["main"]["temp"]),
            "temperatureUnit": "F",
            "windSpeed": f"{int(data['wind']['speed'])} mph", # Spark regex expects "X mph"
            "windDirection": deg_to_compass(data.get("wind", {}).get("deg", 0)),
            "shortForecast": data["weather"][0]["description"],
            "probabilityOfPrecipitation": {
                "unitCode": "wmoUnit:percent", 
                "value": 0  # Defaulting to 0 for current observations
            },
            "borough": borough,
            "source_lat": lat,
            "source_lon": lon,
        }

        kafka_key = f"{borough}::{event_time}"

        producer.produce(
            topic=TOPIC_NAME,
            key=kafka_key.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            callback=delivery_report,
        )

        producer.flush()
        print(f"[{borough}] Pushed current weather (Temp: {payload['temperature']}F) to '{TOPIC_NAME}'.")

    except requests.exceptions.RequestException as e:
        print(f"[{borough}] Forecast fetch error: {e}")
    except Exception as e:
        print(f"[{borough}] Unexpected error: {e}")

def fetch_all_boroughs():
    print("--- Polling OpenWeatherMap for all NYC boroughs ---")
    for borough, coords in NYC_BOROUGHS.items():
        fetch_and_send_borough(borough, coords["lat"], coords["lon"])
        time.sleep(1)  # Rate-limit courtesy

if __name__ == "__main__":
    print(f"Starting NYC Weather Producer (OpenWeatherMap). Polling every {POLL_INTERVAL}s...")
    while True:
        fetch_all_boroughs()
        time.sleep(POLL_INTERVAL)