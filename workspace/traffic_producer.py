import requests
import json
import time
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

APP_TOKEN    = os.getenv("NYC_DOT_APP_TOKEN")
KAFKA_BROKER = "redpanda:29092"
TOPIC_NAME   = "nyc_traffic_raw"
POLL_INTERVAL = 60  # seconds

BASE_URL = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"

producer = Producer({
    "bootstrap.servers": KAFKA_BROKER,
    "client.id": "nyc-traffic-producer"
})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for key {msg.key()}: {err}")

def fetch_and_send(last_seen_ts: str) -> str:
    """
    Fetches records newer than last_seen_ts from NYC DOT API and pushes to Kafka.
    Uses the params dict so requests handles encoding correctly, preventing
    SoQL malformed query errors caused by pre-built URL strings.
    """
    headers = {"X-App-Token": APP_TOKEN} if APP_TOKEN else {}

    params = {
        "$limit": 1000,
        "$order": "data_as_of DESC",
    }

    if last_seen_ts and last_seen_ts.strip():
        clean_ts = last_seen_ts.split(".")[0]  # strip microseconds
        params["$where"] = f"data_as_of > '{clean_ts}'"

    try:
        print(f"Fetching traffic data (since {last_seen_ts or 'beginning'})...")
        response = requests.get(BASE_URL, headers=headers, params=params, timeout=30)

        if response.status_code == 400:
            print(f"[DEBUG] Malformed request URL: {response.url}")
            print(f"[DEBUG] Response body: {response.text}")

        response.raise_for_status()
        records = response.json()

        if not records:
            print("No new records since last poll.")
            return last_seen_ts

        new_latest_ts = last_seen_ts
        for record in records:
            sensor_id = str(record.get("id", "unknown"))
            payload   = json.dumps(record)

            # Push to Redpanda
            producer.produce(
                topic=TOPIC_NAME,
                key=sensor_id.encode("utf-8"),
                value=payload.encode("utf-8"),
                callback=delivery_report,
            )

            # Track the most recent timestamp to use for the next poll
            record_ts = record.get("data_as_of")
            if record_ts and (not new_latest_ts or record_ts > new_latest_ts):
                new_latest_ts = record_ts

        producer.flush()
        print(f"Pushed {len(records)} new records to '{TOPIC_NAME}'.")
        return new_latest_ts

    except requests.exceptions.RequestException as e:
        print(f"API Connection Error: {e}")
        return last_seen_ts
    except Exception as e:
        print(f"Unexpected Producer Error: {e}")
        return last_seen_ts

if __name__ == "__main__":
    print(f"Starting NYC Traffic Producer. Polling every {POLL_INTERVAL}s...")
    last_seen_ts = ""  # empty = fetch everything on first run
    while True:
        last_seen_ts = fetch_and_send(last_seen_ts)
        time.sleep(POLL_INTERVAL)