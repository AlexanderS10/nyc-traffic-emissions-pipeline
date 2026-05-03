# Troubleshooting Guide

## Checking Container Health

```bash
# View status of all containers
docker compose ps

# Tail logs for a specific service
docker logs rest-catalog --tail=30
docker logs spark-master --tail=50
docker logs spark-worker --tail=50
docker logs redpanda --tail=30
```

## Redpanda / Kafka Issues

```bash
# List all topics and their partition offsets
docker exec -it redpanda rpk topic list

# Check consumer group lag
docker exec -it redpanda rpk group list
docker exec -it redpanda rpk group describe <group-id>

# Reset a topic (nuclear option - deletes all messages)
docker exec -it redpanda rpk topic delete nyc_traffic_raw
docker exec -it redpanda rpk topic create nyc_traffic_raw
```

## MinIO / S3 Issues

```bash
# Set up the mc alias (required once per terminal session)
# Use MinIO root credentials from your repo `.env`.
docker exec -it minio mc alias set myminio http://localhost:9000 <MINIO_ROOT_USER> <MINIO_ROOT_PASSWORD>

# Browse the medallion buckets
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

**Static data onboarding utility**

```bash
# Run automated static onboarding (download + MinIO upload)
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/initialize_static_data.py

# If rerunning and you need to overwrite existing objects:
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/initialize_static_data.py --overwrite

# Verify expected prefixes are populated
docker exec -it minio mc ls --recursive myminio/raw-data/static/
```

## Spark Streaming Issues

**StreamingQueryException - AWS region/credentials error:**

Ensure `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_REGION` are set in `docker-compose.yml` for `spark-master`, `spark-worker`, and `jupyter` services. After editing compose:

```bash
docker compose down
docker compose up -d
```

**StreamingQueryException - Redefining watermark:**

Remove `.withWatermark()` from individual stream DataFrames and apply it only once after `.unionByName()` on the merged air quality stream.

**Streaming job ran but table is empty:**

The watermark window requires all streams to have overlapping event timestamps. With `startingOffsets: earliest`, historical data catches up quickly. Wait for at least one full micro-batch to commit (about 30 seconds) before querying.

**Data latency note:**

Spark uses stateful watermarking for stream-to-stream joins, so records typically appear in the Iceberg table about 15-20 minutes after ingestion. That delay ensures the traffic, air quality, and weather joins complete with the necessary temporal context.

**Stale checkpoint causing schema mismatch:**

```bash
# Clear all checkpoints and restart from the top
docker exec -it minio mc rm -r --force myminio/raw-data/checkpoints/
docker exec -it minio mc rm -r --force myminio/refined-data/checkpoints/
docker exec -it minio mc rm -r --force myminio/business-data/checkpoints/

# Then restart the Jupyter kernel and re-run all cells
```

## Iceberg / Trino Issues

```bash
# Test that the REST catalog is reachable
curl http://localhost:8181/v1/namespaces
curl http://localhost:8181/v1/config

# Connect to Trino CLI
docker exec -it trino trino

# Inside Trino - show catalogs and schemas
SHOW CATALOGS;
SHOW SCHEMAS FROM iceberg;
SHOW TABLES FROM iceberg.db;
```

**Trino 403 Forbidden on S3:**

Ensure `iceberg.properties` in `trino_config/` contains the correct MinIO endpoint and credentials and that the file is mounted into the Trino container in `docker-compose.yml`.