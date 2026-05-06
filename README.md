# NYC Real-Time Traffic & Air Quality Emissions Pipeline

**NYU CS-GY-6513: Big Data — Final Project**  
*Brian Cheng · Uma Nachiappan · Alex Sanchez · Josue Flores*

## Project Overview

City residents often hear that traffic worsens air pollution, but most research relies on historical, offline datasets that fail to capture real-time city dynamics. Urban air quality is heavily skewed by confounding factors like weather patterns, building heating systems, and diesel truck routes.

This project builds a highly scalable, real-time distributed data pipeline that isolates the environmental cost of traffic congestion. We execute a multi-stream join that correlates NYC Real-Time Traffic Speeds with OpenAQ air quality measurements within a 15-minute sliding window to quantify how local air quality degrades during neighborhood traffic jams.

## Architecture

```
NYC DOT API ──┐
OpenAQ API  ──┼──► Redpanda (Kafka) ──► PySpark Structured Streaming ──► MinIO (Iceberg) ──► Trino ──► Grafana
OpenWeatherMap API ──┘    Topics                  H3 Spatial Join
```

1. Ingestion (Redpanda/Kafka): Python producers poll NYC DOT, OpenAQ, PurpleAir, and OpenWeatherMap APIs and publish JSON payloads into dedicated Kafka topics.
2. Stream Processing (PySpark 4.0.2): Spark Structured Streaming consumes topics, applies event-time watermarking, and assigns H3 hex grid IDs (resolution 7) for spatial indexing.
3. Multi-Stream Join: Traffic nodes and air sensors sharing the same H3 grid cell are joined within a 15-minute sliding window. Weather data joins on a 15-minute window using a broadcast key. Apache Sedona performs spatial joins between live traffic points and static polygon layers to isolate confounding variables.
4. Medallion Storage (MinIO + Apache Iceberg): The pipeline writes into three buckets: `raw-data` for raw Kafka JSON landed as Parquet, `refined-data` for cleaned and spatially indexed Parquet, and `business-data` for the final Apache Iceberg table.
5. Serving Layer (Trino + Grafana): Trino queries the Iceberg tables in `business-data`, powering a live Grafana dashboard with borough-level traffic and air quality panels.

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.11 |
| Message Broker | Redpanda v24.2.7 (Kafka API compatible) |
| Stream Processing | Apache Spark 4.0.2 (Structured Streaming) |
| Geospatial Indexing | H3 (Uber hex grid), Apache Sedona |
| Data Lake Storage | MinIO (S3-compatible, medallion buckets: `raw-data`, `refined-data`, `business-data`) |
| Table Format | Apache Iceberg 1.10.1 |
| REST Catalog | apache/iceberg-rest-fixture |
| Query Engine | Trino 480 |
| Visualization | Grafana 11.4.0 |

## Data Sources

**Primary Real-Time Streams:**
- [NYC DOT Real-Time Traffic Speeds](https://data.cityofnewyork.us/Transportation/DOT-Traffic-Speeds-NBE/i4gi-tjb9) — polled every 60 seconds
- [OpenAQ API v3](https://docs.openaq.org) — PM2.5 sensor readings, polled every 5 minutes
- [PurpleAir API](https://develop.purpleair.com) — hyperlocal PM2.5, polled every 2 minutes
- [OpenWeatherMap Current Weather API](https://openweathermap.org/current) — weather observations, polled every 5 minutes

**Static Lookup Datasets:**
- [NYC Truck Route Network: NYC Open Data - Truck Routes](https://data.cityofnewyork.us/Transportation/New-York-City-Truck-Routes-Map-/wnu3-egq7) (download as Shapefile)
- [MTA Congestion Relief Zone (Geofence): MTA Central Business District Geofence](https://data.ny.gov/Transportation/MTA-Central-Business-District-Geofence-Beginning-J/srxy-5nxn/about_data) (download as GeoJSON)
- [NYC LL84 Building Energy Data: NYC Open Data - Local Law 84](https://data.cityofnewyork.us/Environment/Energy-and-Water-Data-Disclosure-for-Local-Law-84-/usc3-8zwd/about_data) (download as CSV)


## Prerequisites

- Docker Desktop with at least 8GB RAM allocated (16GB recommended)
- Docker Compose v2+

## Setup

### 1. Clone the Repository

```bash
git clone git@github.com:AlexanderS10/nyc-traffic-emissions-pipeline.git
cd nyc-traffic-emissions-pipeline
```

### 2. Configure Environment Variables

Create both environment files from templates:

```bash
cp .env.example .env
cp workspace/.env.example workspace/.env
```

- `.env` is used by Docker Compose for infrastructure credentials (MinIO root/app keys and AWS region).
- `workspace/.env` is used by the data producers for API tokens.

Add your API credentials to `workspace/.env`:

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

This builds the custom PySpark 4.0.2 Jupyter image and starts all services. The first build takes about 5–10 minutes.

### 4. MinIO Buckets and App Credentials

The `minio-init` container creates the `raw-data`, `refined-data`, and `business-data` buckets automatically on startup.
It also provisions the application-scoped MinIO user (`MINIO_APP_ACCESS_KEY` / `MINIO_APP_SECRET_KEY`) defined in `.env`, which is used by Spark, Trino, and Iceberg REST.

### 5. Static Data Onboarding

Preferred (automated): run the one-shot onboarding utility inside Jupyter:

```bash
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E2_initialize_static_data.py
```

The script downloads:
- NYC Truck Routes (Shapefile bundle)
- MTA Congestion Zones (GeoJSON)
- LL84 Building Energy (CSV)

and uploads them to:

```text
s3a://raw-data/static/truck_routes/
s3a://raw-data/static/congestion_zones/
s3a://raw-data/static/building_energy/
```

Manual fallback:

Upload the static datasets to `s3a://raw-data/static/` from the MinIO Console at `http://localhost:9001` using your `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` values from `.env`.

```text
s3a://raw-data/static/truck_routes/
s3a://raw-data/static/congestion_zones/
s3a://raw-data/static/building_energy/
```

Use NYC Truck Routes as a Shapefile (`.shp` bundle), MTA Congestion Zones as GeoJSON (`.geojson`), and LL84 building energy data as CSV (`.csv`). The Shapefile and GeoJSON assets are used by Apache Sedona for polygon-based spatial joins.

Or run the following CLI commands (replace placeholders with your `.env` values):

```
docker exec -it minio mc alias set myminio http://localhost:9000 <MINIO_ROOT_USER> <MINIO_ROOT_PASSWORD>
docker cp "workspace/static/." minio:/tmp/static
docker exec -it minio mc cp --recursive /tmp/static/truck_routes myminio/raw-data/static/
docker exec -it minio mc cp --recursive /tmp/static/congestion_zones myminio/raw-data/static/
docker exec -it minio mc cp --recursive /tmp/static/building_energy myminio/raw-data/static/
docker exec -it minio mc ls --recursive myminio/raw-data/static/
```

### 6. Verify All Services Are Running

```bash
docker compose ps
```

All containers should show `running`. Expected services:

| Service | URL | Credentials |
|---|---|---|
| Jupyter Lab | http://localhost:8888?token=bigdata | token: `bigdata` |
| Redpanda Console | http://localhost:8080 | — |
| Spark Master UI | http://localhost:8082 | — |
| MinIO Console | http://localhost:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` |
| Grafana | http://localhost:3000 | admin / admin |
| Trino UI | http://localhost:8081 | — |
| Redpanda | localhost:9092 | — |
| Iceberg REST Catalog | http://localhost:8181 | — |

### 7. (Optional) Local Python Virtual Environment

If you want to run utility scripts from your host machine:

```bash
python -m venv .venv
# Windows PowerShell:
.venv\Scripts\Activate.ps1
# macOS/Linux:
source .venv/bin/activate
pip install -r requirements.txt
python workspace/scripts/E1_check_imports.py
```

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

Open Jupyter at http://localhost:8888?token=bigdata and open `01_nyc_environmental_pipeline.ipynb`.

Run all cells in order. The final cell starts the streaming write to Iceberg and will run continuously. You will see:

```text
Spark Session Ready! Version: 4.0.2
```

The streaming query commits micro-batches every 30 seconds. Monitor progress in the Spark UI at http://localhost:4040 while the streaming job is running from Jupyter.

### Step 4: Query the Live Data

Open `02_live_data_exploration.ipynb` in a separate Jupyter tab while the pipeline notebook remains running.

Run the query cells to inspect the Iceberg table:

```python
spark.sql("""
    SELECT traffic_borough, COUNT(*) as records, AVG(traffic_speed) as avg_speed
    FROM local.db.enriched_traffic
    GROUP BY traffic_borough
    ORDER BY avg_speed DESC
""").show()
```

### Step 5: Epic 4 End-to-End Validation Runbook

Use this sequence when validating the full Epic 4 path (traffic + AQ + weather + static flags):

1. Start producers (`workspace/run_all.py`) and keep them running.
2. Open `workspace/01_nyc_environmental_pipeline.ipynb` and run all cells in order.
3. Keep the final stream-monitor cell running and watch `[stream_metrics]` logs for:
   - `batch_id`
   - `numInputRows`
   - `processedRowsPerSecond`
   - `trigger_execution_ms`
4. Open `workspace/02_live_data_exploration.ipynb` and run all diagnostics cells.
5. Run Epic 4 verification scripts from the `jupyter-pyspark` container:

```bash
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E4_m3_state_recovery_verification.py
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E4_m4_weather_enrichment_verification.py
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E4_m5_static_lookup_verification.py
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E4_m6_static_enrichment_verification.py
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E4_m7_acceptance_snapshot.py
```

Current local defaults used for reviewed runs:

- H3 resolution: `7`
- AQ join window: `15 minutes`
- Weather join window: `15 minutes`
- Business checkpoint path: `s3a://business-data/checkpoints/local.db.enriched_traffic_v4`

## Project Documentation

- [REPORT.md](docs/REPORT.md) - Final Written Report & Analytical Findings
- [DASHBOARD.md](docs/DASHBOARD.md) - Grafana Setup & UI Configuration
- [TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) - Debugging & Maintenance Guide