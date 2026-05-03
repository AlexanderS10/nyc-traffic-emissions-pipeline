import requests
import json
import time
from datetime import datetime, timezone
from producer_common import TOPICS, create_producer, get_logger, get_required_env, get_json_with_retry, log_startup

OPENWEATHER_API_KEY = get_required_env("OPENWEATHER_API_KEY")
TOPIC_NAME = TOPICS["weather"]
POLL_INTERVAL = 300  # 5 minutes! This gives us near real-time weather ingestion.

# Geographic centroids for each NYC borough
NYC_BOROUGHS = {
    "manhattan":     {"lat": 40.7831, "lon": -73.9712},
    "brooklyn":      {"lat": 40.6782, "lon": -73.9442},
    "queens":        {"lat": 40.7282, "lon": -73.7949},
    "bronx":         {"lat": 40.8448, "lon": -73.8648},
    "staten_island": {"lat": 40.5795, "lon": -74.1502},
}

logger = get_logger("weather_producer")
producer = create_producer("nyc-weather-producer")

def delivery_report(err, msg):
    if err is not None:
        logger.error("kafka_delivery_failed key=%s error=%s", msg.key(), err)

def deg_to_compass(num):
    """Converts wind direction in degrees to a compass direction (NOAA style)."""
    val = int((num / 22.5) + .5)
    arr = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return arr[(val % 16)]

def fetch_and_send_borough(borough: str, lat: float, lon: float):
    # Call the OpenWeatherMap Current Weather endpoint
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPENWEATHER_API_KEY}&units=imperial"
    
    try:
        data = get_json_with_retry(logger, url)

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
        logger.info(
            "kafka_weather_sent topic=%s borough=%s temperature_f=%s",
            TOPIC_NAME,
            borough,
            payload["temperature"],
        )

    except requests.exceptions.RequestException as e:
        logger.error("forecast_fetch_error borough=%s error=%s", borough, e)
    except Exception as e:
        logger.error("unexpected_error borough=%s error=%s", borough, e)

def fetch_all_boroughs():
    logger.info("fetch_weather_all_boroughs")
    for borough, coords in NYC_BOROUGHS.items():
        fetch_and_send_borough(borough, coords["lat"], coords["lon"])
        time.sleep(1)  # Rate-limit courtesy

if __name__ == "__main__":
    log_startup(logger, "nyc-weather-producer", TOPIC_NAME, POLL_INTERVAL)
    while True:
        fetch_all_boroughs()
        time.sleep(POLL_INTERVAL)