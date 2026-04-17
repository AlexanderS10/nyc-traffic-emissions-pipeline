# NYC Real-Time Traffic & Air Quality Emissions Pipeline

**NYU CS-GY-6513: Big Data - Final Project**
*Team Members: Brian Cheng, Uma Nachiappan, Alex Sanchez, Josue Flores*

## Project Overview

City residents often hear that traffic worsens air pollution, but most research relies on historical, offline datasets that fail to capture real-time city dynamics. Urban air quality is heavily skewed by confounding factors like weather patterns, building heating systems, and diesel truck routes.

The core objective of this project is to build a highly scalable, real-time distributed data pipeline that isolates the true environmental cost of traffic congestion. We execute a complex multi-stream enriched join, correlating NYC Real-Time Traffic Speeds with OpenAQ air quality measurements within a 15-minute sliding window to quantify exactly how much local air quality degrades per minute during a neighborhood traffic jam.

## Architecture & Data Flow

This project implements a modern Data Lakehouse streaming architecture:

1. **Ingestion (Redpanda / Kafka):** Python producer scripts continuously poll the NYC DOT, OpenAQ, and NOAA APIs, publishing high-velocity JSON payloads into dedicated message queues.
2. **Stream Processing (PySpark 4.0.2):** Spark Structured Streaming consumes the raw topics, applies event-time watermarking, and assigns H3 hex grid IDs for rapid spatial indexing.
3. **Complex Geospatial Joins (Apache Sedona):** PySpark correlates traffic nodes and air sensors sharing grid IDs within a 15-minute window, simultaneously cross-referencing static S3 shapefiles (NYC Truck Routes, Congestion Zones) to filter out background noise.
4. **Resilient Storage (MinIO S3 + Apache Iceberg):** The fully enriched stream is continuously written to an S3-compatible Data Lake layered with Apache Iceberg for ACID compliance and schema evolution.
5. **Serving Layer (Trino + Grafana):** A Trino SQL engine sits on top of the Iceberg tables, powering a live Grafana dashboard featuring real-time borough heatmaps and time-series correlation charts.

## Tech Stack

* **Language:** Python 3
* **Message Broker:** Redpanda (Kafka API)
* **Stream Processing:** Apache Spark (Structured Streaming)
* **Geospatial SQL:** Apache Sedona
* **Data Lake Storage:** MinIO (AWS S3 alternative)
* **Table Format:** Apache Iceberg
* **Query Engine:** Trino
* **Visualization:** Grafana

## Data Sources

**Primary High-Velocity Streams:**

* [NYC DOT Real-Time Traffic Speeds](https://data.cityofnewyork.us/Transportation/DOT-Traffic-Speeds-NBE/i4gi-tjb9)
* [OpenAQ Air Quality API (v3)](https://docs.openaq.org) (https://explore.openaq.org/register)
* [NOAA National Weather Service API](https://www.weather.gov/documentation/services-web-api)

**Static / Lookup Datasets:**

* NYC Truck Route Network Shapefiles
* MTA Congestion Relief Zones
* NYC LL84 Building Energy Benchmarking Data

---

## Local Setup & Development

This project relies on a fully containerized Docker infrastructure to eliminate dependency conflicts.

### 1. Prerequisites

* Docker Desktop installed and running.
* At least 8GB of Docker memory allocated (16GB recommended for Spark).

### 2. Configure API Keys

Create a `.env` file inside the `workspace/` directory to store your API credentials securely:

```env
NYC_DOT_APP_TOKEN=your_token_here
OPENAQ_API_KEY=your_key_here
```

### 3. Start the Big Data Cluster

```bash
git clone git@github.com:YOUR_USERNAME/nyc-traffic-emissions-pipeline.git
cd nyc-traffic-emissions-pipeline
docker compose up -d --build
```

This command builds the custom PySpark 4.0.2 Jupyter image and spins up Redpanda, Spark Master/Worker, MinIO, Trino, and Grafana.

### 4. Access the Services

Jupyter Lab (PySpark IDE): ```http://localhost:8888/lab?token=bigdata```

Spark Master UI: ```http://localhost:8080```

MinIO Console (S3): ```http://localhost:9001``` (admin / password) !Create the bucker also called "data-lake"

Grafana Dashboard: ```http://localhost:3000``` (admin / admin)

### 5. Executing the Data Pipeline

To start streaming data into the Redpanda message broker, the producer scripts must be executed from inside the containerized environment.

1. Open a terminal and access the Jupyter container:

   ```bash
   docker exec -it jupyter-pyspark bash
   ```

1. Navigate to the mapped workspace directory:

   ```bash
   cd /home/jovyan/work
   ```

1. Execute the producer script:

   ```bash
   python traffic_producer.py
   ```

### 6. Verifying the Data Stream

You can verify the data is flowing by inspecting the Redpanda topic in a separate terminal on your local machine:

```bash
docker exec -it redpanda rpk topic consume nyc_traffic_raw
```
