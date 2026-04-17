import requests
import json
import time
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load API keys from the .env file
load_dotenv()

OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY")

# If running this script directly on your Mac, use localhost:9092
# If running inside the Jupyter docker container, use redpanda:29092
KAFKA_BROKER = "redpanda:29092"
TOPIC_NAME   = "nyc_openaq_raw"
POLL_INTERVAL = 300  # 5 minutes (air quality doesn't change by the second)

# OpenAQ v3 API endpoint for NYC (using a bounding box or coordinates for NYC)
# This example uses coordinates roughly bounding New York City
API_URL = "https://api.openaq.org/v3/locations"
PARAMS = {
    "coordinates": "40.7128,-74.0060",
    "radius": 25000, # 25km radius around NYC
    "limit": 100
}

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "nyc-openaq-producer"
})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key {msg.key()}: {err}")

def fetch_and_send():
    """
    Fetches the latest air quality readings for NYC and pushes them to Kafka.
    """
    if not OPENAQ_API_KEY:
        print("Error: OPENAQ_API_KEY not found in .env file.")
        return

    headers = {
        "X-API-Key": OPENAQ_API_KEY,
        "Accept": "application/json"
    }

    try:
        print("Fetching OpenAQ data for NYC...")
        response = requests.get(API_URL, headers=headers, params=PARAMS, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # OpenAQ v3 typically returns a "results" array
        records = data.get("results", [])

        if not records:
            print("No records found in this polling window.")
            return

        for record in records:
            # Use the location ID as the Kafka key for partitioning
            location_id = str(record.get("id", "unknown"))
            payload   = json.dumps(record)

            producer.produce(
                topic=TOPIC_NAME,
                key=location_id.encode("utf-8"),
                value=payload.encode("utf-8"),
                callback=delivery_report,
            )

        producer.flush()
        print(f"Pushed {len(records)} location records to '{TOPIC_NAME}'.")

    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")

if __name__ == "__main__":
    print(f"Starting NYC OpenAQ Producer. Polling every {POLL_INTERVAL}s...")
    while True:
        fetch_and_send()
        time.sleep(POLL_INTERVAL)
# %%
