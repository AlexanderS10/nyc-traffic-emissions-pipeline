import json
import time
from datetime import datetime, timezone
import requests

from producer_common import TOPICS, create_producer, get_logger, get_required_env, get_json_with_retry, log_startup

APP_TOKEN = get_required_env("NYC_DOT_APP_TOKEN")
TOPIC_NAME = TOPICS["traffic"]
POLL_INTERVAL = 60  # seconds

BASE_URL = "https://data.cityofnewyork.us/resource/i4gi-tjb9.json"

logger = get_logger("traffic_producer")
producer = create_producer("nyc-traffic-producer")

def delivery_report(err, msg):
    if err is not None:
        logger.error("kafka_delivery_failed key=%s error=%s", msg.key(), err)

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
        logger.info("fetch_traffic since=%s", last_seen_ts or "beginning")
        records = get_json_with_retry(
            logger,
            BASE_URL,
            headers=headers,
            params=params,
        )

        if not records:
            logger.info("no_new_records")
            return last_seen_ts

        new_latest_ts = last_seen_ts
        fetch_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for record in records:
            payload   = json.dumps(record)

            # Push to Redpanda
            producer.produce(
                topic=TOPIC_NAME,
                key=fetch_ts.encode("utf-8"),
                value=payload.encode("utf-8"),
                callback=delivery_report,
            )

            # Track the most recent timestamp to use for the next poll
            record_ts = record.get("data_as_of")
            if record_ts and (not new_latest_ts or record_ts > new_latest_ts):
                new_latest_ts = record_ts

        producer.flush()
        logger.info("kafka_batch_sent topic=%s records=%s", TOPIC_NAME, len(records))
        return new_latest_ts

    except requests.exceptions.RequestException as e:
        logger.error("api_connection_error error=%s", e)
        return last_seen_ts
    except Exception as e:
        logger.error("unexpected_producer_error error=%s", e)
        return last_seen_ts

if __name__ == "__main__":
    log_startup(logger, "nyc-traffic-producer", TOPIC_NAME, POLL_INTERVAL)
    last_seen_ts = ""  # empty = fetch everything on first run
    while True:
        last_seen_ts = fetch_and_send(last_seen_ts)
        time.sleep(POLL_INTERVAL)