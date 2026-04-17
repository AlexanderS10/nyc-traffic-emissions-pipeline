import requests
import json
import time
import os
from dotenv import load_dotenv
from confluent_kafka import Producer

# Load API keys from the .env file
load_dotenv()

PURPLEAIR_API_KEY = os.getenv("PURPLEAIR_API_KEY")
KAFKA_BROKER = "redpanda:29092"
TOPIC_NAME   = "nyc_purpleair_raw"
POLL_INTERVAL = 120  # 2 minutes (PurpleAir updates relatively frequently)

# PurpleAir v1 API endpoint
API_URL = "https://api.purpleair.com/v1/sensors"

# Bounding box roughly covering New York City (NW to SE)
PARAMS = {
    "fields": "name,latitude,longitude,pm2.5_10minute,temperature,humidity",
    "nwlng": "-74.2590",
    "nwlat": "40.9176",
    "selng": "-73.7003",
    "selat": "40.4774"
}

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "nyc-purpleair-producer"
})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key {msg.key()}: {err}")

def fetch_and_send():
    """
    Fetches the latest air quality readings for NYC sensors and pushes them to Kafka.
    """
    if not PURPLEAIR_API_KEY:
        print("Error: PURPLEAIR_API_KEY not found in .env file.")
        return

    headers = {
        "X-API-Key": PURPLEAIR_API_KEY
    }

    try:
        print("Fetching PurpleAir data for NYC...")
        response = requests.get(API_URL, headers=headers, params=PARAMS, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # PurpleAir returns a "data" array of arrays, and a "fields" array explaining the columns
        fields = data.get("fields", [])
        sensor_data = data.get("data", [])

        if not sensor_data:
            print("No sensor data found in this polling window.")
            return

        # Convert the arrays into JSON objects for easier processing in Spark later
        for row in sensor_data:
            record = dict(zip(fields, row))
            # Use the sensor index as the Kafka key
            sensor_id = str(record.get("sensor_index", "unknown"))
            payload = json.dumps(record)

            producer.produce(
                topic=TOPIC_NAME,
                key=sensor_id.encode("utf-8"),
                value=payload.encode("utf-8"),
                callback=delivery_report,
            )

        producer.flush()
        print(f"Pushed {len(sensor_data)} PurpleAir records to '{TOPIC_NAME}'.")

    except requests.exceptions.RequestException as e:
        print(f"API Error: {e}")
    except Exception as e:
        print(f"Unexpected Error: {e}")

if __name__ == "__main__":
    print(f"Starting NYC PurpleAir Producer. Polling every {POLL_INTERVAL}s...")
    while True:
        fetch_and_send()
        time.sleep(POLL_INTERVAL)