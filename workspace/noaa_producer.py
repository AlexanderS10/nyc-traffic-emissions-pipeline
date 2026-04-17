import requests
import json
import time
import os
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load variables from .env
load_dotenv()

KAFKA_BROKER = "redpanda:29092"
TOPIC_NAME   = "nyc_weather_raw"
POLL_INTERVAL = 1800  # 30 minutes (Weather updates slowly, no need to spam the API)

# NOAA API endpoint for NYC hourly forecast (Grid OKX/32,34 covers lower Manhattan)
API_URL = "https://api.weather.gov/gridpoints/OKX/32,34/forecast/hourly"

# IMPORTANT: Replace this email with your own. NOAA requires this.
HEADERS = {
    "User-Agent": "NYCAirQualityProject (your_email@example.com)",
    "Accept": "application/geo+json"
}

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "nyc-weather-producer"
})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key {msg.key()}: {err}")

def fetch_and_send():
    """
    Fetches the latest hourly weather forecast for NYC and pushes it to Kafka.
    """
    try:
        print("Fetching NOAA Weather data for NYC...")
        response = requests.get(API_URL, headers=HEADERS, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # NOAA returns periods (hours). Let's grab the current and next few hours.
        periods = data.get("properties", {}).get("periods", [])

        if not periods:
            print("No weather periods found in this response.")
            return

        # We'll push the first 5 periods (next 5 hours) to the stream
        for period in periods[:5]:
            # Use the forecast time as the Kafka key
            forecast_time = str(period.get("startTime", "unknown"))
            payload = json.dumps(period)

            producer.produce(
                topic=TOPIC_NAME,
                key=forecast_time.encode("utf-8"),
                value=payload.encode("utf-8"),
                callback=delivery_report,
            )

        producer.flush()
        print(f"Pushed {len(periods[:5])} hourly weather forecasts to '{TOPIC_NAME}'.")

    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")

if __name__ == "__main__":
    print(f"Starting NYC Weather Producer. Polling every {POLL_INTERVAL}s...")
    while True:
        fetch_and_send()
        time.sleep(POLL_INTERVAL)