# NYC Real-Time Traffic & Air Quality Emissions Pipeline

**NYU CS-GY-6513: Big Data — Final Project**  
*Brian Cheng · Uma Nachiappan · Alex Sanchez · Josue Flores*

---

## Project Overview

City residents often hear that traffic worsens air pollution, but most research relies on historical, offline datasets that fail to capture real-time city dynamics. Urban air quality is heavily skewed by confounding factors like weather patterns, building heating systems, and diesel truck routes.

This project builds a highly scalable, real-time distributed data pipeline that isolates the true environmental cost of traffic congestion. We execute a complex multi-stream enriched join, correlating NYC Real-Time Traffic Speeds with OpenAQ air quality measurements within a 15-minute sliding window to quantify exactly how much local air quality degrades per minute during a neighborhood traffic jam.

---

## Architecture & Data Flow

```
NYC DOT API ──┐
OpenAQ API  ──┼──► Redpanda (Kafka) ──► PySpark Structured Streaming ──► MinIO (Iceberg) ──► Trino ──► Grafana
OpenWeatherMap API ──┘    Topics                  H3 Spatial Join
```

1. **Ingestion (Redpanda/Kafka):** Python producers continuously poll NYC DOT, OpenAQ, PurpleAir, and OpenWeatherMap APIs, publishing JSON payloads into dedicated Kafka topics.
2. **Stream Processing (PySpark 4.0.2):** Spark Structured Streaming consumes topics, applies event-time watermarking, and assigns H3 hex grid IDs (resolution 5) for spatial indexing.
3. **Multi-Stream Join:** Traffic nodes and air sensors sharing the same H3 grid cell are joined within a 15-minute sliding window. Weather data joins on a 15-minute window using a broadcast key.
4. **Medallion Storage (MinIO + Apache Iceberg):** The pipeline writes into three distinct buckets: `raw-data` for raw Kafka JSON landed as Parquet, `refined-data` for cleaned and spatially indexed Parquet, and `business-data` for the final ACID-compliant Apache Iceberg table.
5. **Serving Layer (Trino + Grafana):** Trino queries the Iceberg tables in `business-data`, powering a live Grafana dashboard with borough-level traffic and air quality panels.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Message Broker | Redpanda v24.2.7 (Kafka API compatible) |
| Stream Processing | Apache Spark 4.0.2 (Structured Streaming) |
| Geospatial Indexing | H3 (Uber hex grid), Apache Sedona |
| Data Lake Storage | MinIO (S3-compatible, Medallion buckets: `raw-data`, `refined-data`, `business-data`) |
| Table Format | Apache Iceberg 1.10.1 |
| REST Catalog | apache/iceberg-rest-fixture |
| Query Engine | Trino 480 |
| Visualization | Grafana 11.4.0 |

---

## Data Sources

**Primary Real-Time Streams:**
- [NYC DOT Real-Time Traffic Speeds](https://data.cityofnewyork.us/Transportation/DOT-Traffic-Speeds-NBE/i4gi-tjb9) — polled every 60s
- [OpenAQ API v3](https://docs.openaq.org) — PM2.5 sensor readings, polled every 5 min
- [PurpleAir API](https://develop.purpleair.com) — hyperlocal PM2.5, polled every 2 min
- [OpenWeatherMap Current Weather API](https://openweathermap.org/current) — weather observations, polled every 5 min

**Static Lookup Datasets:**
- NYC Truck Route Network Shapefiles
- MTA Congestion Relief Zones
- NYC LL84 Building Energy Benchmarking Data

---

## Prerequisites

- Docker Desktop with at least **8GB RAM** allocated (16GB recommended)
- Docker Compose v2+

---

## Setup & Deployment

### 1. Clone the Repository

```bash
git clone git@github.com:AlexanderS10/nyc-traffic-emissions-pipeline.git
cd nyc-traffic-emissions-pipeline
```

### 2. Configure API Keys

Create a `.env` file inside the `workspace/` directory:

```bash
touch workspace/.env
```

Add your credentials:

```env
NYC_DOT_APP_TOKEN=your_token_here
OPENAQ_API_KEY=your_key_here
PURPLEAIR_API_KEY=your_key_here
OPENWEATHER_API_KEY=your_key_here
```

- NYC DOT token: [Register here](https://data.cityofnewyork.us/profile/app_tokens) (free, raises rate limits)
- OpenAQ API key: [Register here](https://explore.openaq.org/register) (free)
- PurpleAir API key: [Register here](https://develop.purpleair.com) (free tier available)
- OpenWeatherMap API key: [Register here](https://openweathermap.org/api) (free tier available)

### 3. Start the Cluster

```bash
docker compose up -d --build
```

This builds the custom PySpark 4.0.2 Jupyter image and starts all services. First build takes ~5–10 minutes.

### 4. MinIO Buckets

The `minio-init` container creates the `raw-data`, `refined-data`, and `business-data` buckets automatically on startup, so no manual MinIO setup is required.

### 5. Verify All Services Are Running

```bash
docker compose ps
```

All containers should show `running`. Expected services:

| Service | URL | Credentials |
|---|---|---|
| Jupyter Lab | http://localhost:8888?token=bigdata | token: `bigdata` |
| Spark Master UI | http://localhost:8080 | — |
| MinIO Console | http://localhost:9001 | admin / password |
| Grafana | http://localhost:3000 | admin / admin |
| Trino UI | http://localhost:8081 | — |
| Redpanda | localhost:9092 | — |
| Iceberg REST Catalog | http://localhost:8181 | — |

---

## Running the Pipeline

### Step 1: Start the Data Producers

Open a terminal into the Jupyter container:

```bash
docker exec -it -w /home/jovyan/work jupyter-pyspark bash
```

Run all producers simultaneously:

```bash
python run_all.py
```

Or run them individually in separate terminals:

```bash
python traffic_producer.py    # NYC DOT — polls every 60s
python openaq_producer.py     # OpenAQ  — polls every 5 min
python purpleair_producer.py  # PurpleAir — polls every 2 min
python noaa_producer.py       # OpenWeatherMap adapter for Spark schema compatibility — polls every 5 min
```

### Step 2: Verify Data Is Flowing Into Redpanda

```bash
# Check topic offsets (should show increasing message counts)
docker exec -it redpanda rpk topic list
docker exec -it redpanda rpk topic describe nyc_traffic_raw

# Consume a few messages to inspect the raw payload
docker exec -it redpanda rpk topic consume nyc_traffic_raw --num 3
docker exec -it redpanda rpk topic consume nyc_weather_raw --num 3
docker exec -it redpanda rpk topic consume nyc_openaq_raw --num 3
```

### Step 3: Run the Spark Streaming Pipeline

Open Jupyter at **http://localhost:8888?token=bigdata** and open `01_nyc_environmental_pipeline.ipynb`.

Run all cells in order. The final cell starts the streaming write to Iceberg and will run continuously — this is expected. You will see:

```
Spark Session Ready! Version: 4.0.2
```

The streaming query commits micro-batches every ~30 seconds. You can monitor progress in the **Spark UI at http://localhost:4040** (available while a streaming job is running from Jupyter).

### Step 4: Query the Live Data

Open `02_live_data_exploration.ipynb` in a **separate Jupyter tab** (the pipeline notebook must remain running).

Run the query cells to inspect the Iceberg table:

```python
spark.sql("""
    SELECT traffic_borough, COUNT(*) as records, AVG(traffic_speed) as avg_speed
    FROM local.db.enriched_traffic
    GROUP BY traffic_borough
    ORDER BY avg_speed DESC
""").show()
```

---

## Grafana Dashboard Setup

### 1. Install the Trino Plugin

```bash
docker exec -it grafana grafana-cli plugins install grafana-trino-datasource
docker restart grafana
```

### 2. Add Trino as a Data Source

In Grafana (http://localhost:3000), go to **Connections → Data Sources → Add new data source → Trino**.

| Setting | Value |
|---|---|
| Host | `trino:8080` |
| Catalog | `iceberg` |
| Schema | `db` |
| Username | `admin` |

Click **Save & Test**.

### 3. Verify Trino Can Query the Table

```bash
docker exec -it trino trino --catalog iceberg --schema db
```

```sql
-- Confirm table exists
SHOW TABLES;

-- Check row count
SELECT COUNT(*) FROM enriched_traffic;

-- Borough-level summary
SELECT
    traffic_borough,
    COUNT(*) AS records,
    ROUND(AVG(traffic_speed), 2) AS avg_speed_mph,
    MIN(traffic_event_ts) AS first_seen,
    MAX(traffic_event_ts) AS last_seen
FROM enriched_traffic
GROUP BY traffic_borough
ORDER BY avg_speed_mph DESC;
```

### 4. Example Grafana Panel Queries

**Borough Average Speed (Time Series):**
```sql
SELECT
    date_trunc('minute', traffic_event_ts) AS time,
    traffic_borough,
    AVG(traffic_speed) AS avg_speed
FROM iceberg.db.enriched_traffic
WHERE traffic_event_ts >= now() - INTERVAL '2' HOUR
GROUP BY 1, 2
ORDER BY 1
```

**PM2.5 vs Traffic Speed Correlation:**
```sql
SELECT
    date_trunc('minute', traffic_event_ts) AS time,
    AVG(traffic_speed) AS avg_speed,
    AVG(aq_pm25_ugm3) AS avg_pm25
FROM iceberg.db.enriched_traffic
WHERE aq_pm25_ugm3 IS NOT NULL
AND traffic_event_ts >= now() - INTERVAL '2' HOUR
GROUP BY 1
ORDER BY 1
```

**Current Borough Speed Heatmap:**
```sql
SELECT
    traffic_borough,
    ROUND(AVG(traffic_speed), 1) AS avg_speed,
    COUNT(*) AS sensor_count
FROM iceberg.db.enriched_traffic
WHERE traffic_event_ts >= now() - INTERVAL '15' MINUTE
GROUP BY traffic_borough
```

---

## Debugging Guide

### Checking Container Health

```bash
# View status of all containers
docker compose ps

# Tail logs for a specific service
docker logs rest-catalog --tail=30
docker logs spark-master --tail=50
docker logs spark-worker --tail=50
docker logs redpanda --tail=30
```

### Redpanda / Kafka Issues

```bash
# List all topics and their partition offsets
docker exec -it redpanda rpk topic list

# Check consumer group lag
docker exec -it redpanda rpk group list
docker exec -it redpanda rpk group describe <group-id>

# Reset a topic (nuclear option — deletes all messages)
docker exec -it redpanda rpk topic delete nyc_traffic_raw
docker exec -it redpanda rpk topic create nyc_traffic_raw
```

### MinIO / S3 Issues

```bash
# Set up the mc alias (required once per terminal session)
docker exec -it minio mc alias set myminio http://localhost:9000 admin password

# Browse the Medallion buckets
docker exec -it minio mc ls myminio/raw-data/
docker exec -it minio mc ls myminio/refined-data/
docker exec -it minio mc ls myminio/business-data/

# Clear stale Spark checkpoints (required after pipeline errors)
docker exec -it minio mc rm -r --force myminio/raw-data/checkpoints/
docker exec -it minio mc rm -r --force myminio/refined-data/checkpoints/
docker exec -it minio mc rm -r --force myminio/business-data/checkpoints/

# Check bucket size
docker exec -it minio mc du myminio/raw-data
docker exec -it minio mc du myminio/refined-data
docker exec -it minio mc du myminio/business-data
```

### Spark Streaming Issues

**StreamingQueryException — AWS region/credentials error:**
Ensure `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION` are set in `docker-compose.yml` for `spark-master`, `spark-worker`, and `jupyter` services. After editing compose:

```bash
docker compose down
docker compose up -d
```

**StreamingQueryException — Redefining watermark:**
Remove `.withWatermark()` from individual stream DataFrames and apply it only once after `.unionByName()` on the merged air quality stream.

**Streaming job ran but table is empty:**
The watermark window requires all streams to have overlapping event timestamps. With `startingOffsets: earliest`, historical data catches up quickly. Wait for at least one full micro-batch to commit (~30s) before querying.

**Data latency note:** Spark uses stateful watermarking for stream-to-stream joins, so records typically appear in the Iceberg table about 15-20 minutes after ingestion. That delay ensures the traffic, air quality, and weather joins complete with the necessary temporal context.

**Stale checkpoint causing schema mismatch:**
```bash
# Clear all checkpoints and restart from the top
docker exec -it minio mc rm -r --force myminio/raw-data/checkpoints/
docker exec -it minio mc rm -r --force myminio/refined-data/checkpoints/
docker exec -it minio mc rm -r --force myminio/business-data/checkpoints/
# Then restart the Jupyter kernel and re-run all cells
```

### Iceberg / Trino Issues

```bash
# Test that the REST catalog is reachable
curl http://localhost:8181/v1/namespaces

# Connect to Trino CLI
docker exec -it trino trino

# Inside Trino — show catalogs and schemas
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.db;
```

**Trino 403 Forbidden on S3:**
Ensure `iceberg.properties` in `trino_config/` contains correct MinIO endpoint and credentials and that the file is mounted into the Trino container in `docker-compose.yml`.

### Weather Source Pivot

We transitioned the weather ingestion path from NOAA to OpenWeatherMap to satisfy the Velocity pillar of Big Data and keep weather observations on the same cadence as the traffic and air quality sensors. The existing `noaa_producer.py` filename is retained for compatibility, but it now functions as an adapter around OpenWeatherMap data.

---

## Project Structure

```
.
├── docker-compose.yml          # Full cluster definition
├── Dockerfile                  # Custom PySpark 4.0.2 + dependencies image
├── trino_config/
│   └── iceberg.properties      # Trino → Iceberg REST catalog config
└── workspace/                  # Mapped into Jupyter container
    ├── .env                    # API keys (not committed to git)
    ├── run_all.py              # Launches all producers in parallel
    ├── traffic_producer.py     # NYC DOT traffic stream
    ├── openaq_producer.py      # OpenAQ air quality stream
    ├── purpleair_producer.py   # PurpleAir hyperlocal PM2.5 stream
    ├── noaa_producer.py        # OpenWeatherMap adapter for the weather stream
    ├── 01_nyc_environmental_pipeline.ipynb   # Main Spark streaming pipeline
    └── 02_live_data_exploration.ipynb        # Ad-hoc Iceberg queries
```

---

## Known Limitations & Design Notes

**Air quality NULLs:** OpenAQ has sparse sensor coverage in NYC. H3 resolution 5 (~252km² cells) is used to maximize join hits, but some traffic segments will still produce NULL air quality readings due to genuine absence of nearby sensors. This is documented as a data quality finding.

**Weather join NULLs:** OpenWeatherMap observations are now joined on a 15-minute window so weather cadence matches the rest of the pipeline. Traffic events that fall outside the overlapping join window can still produce NULLs when the state store has not yet observed the matching weather record.

**Real-time weather join latency:** The enriched traffic stream uses a left outer join so traffic rows always flow downstream immediately. Because the weather stream is statefully joined with traffic and air quality data, records typically surface in Iceberg about 15-20 minutes after ingestion once Spark has enough watermark progress to confirm join completeness.

**Single Kafka partition:** All topics use a single partition for local development simplicity. Production scale-out would require multiple partitions and corresponding Spark executor parallelism.

**PurpleAir timestamps:** PurpleAir sensors do not return an explicit event timestamp in the bounding box query, so the Kafka ingestion timestamp is used as a proxy for event time. This introduces minor lag in the stream join.