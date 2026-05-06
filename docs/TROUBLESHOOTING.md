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
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E2_initialize_static_data.py

# If rerunning and you need to overwrite existing objects:
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E2_initialize_static_data.py --overwrite

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

**Epic 3 — Milestone 3: late-event watermark verification**

```bash
# 1) Inject a deliberately late traffic event (older than watermark)
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E3_m3_inject_late_traffic_payload.py --minutes-late 90 --record-id late-probe-001

# 2) Verify raw present + enriched absent + watermark snapshot proxy
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E3_m3_watermark_verification.py --record-id late-probe-001
```

Expected:
- Raw check finds `late-probe-001`
- Enriched check returns zero rows for `traffic_id = late-probe-001`
- Snapshot prints recent event-time ranges / freshness metrics

**Epic 3 — Milestone 4: data-quality verification**

```bash
# Verifies invalid-speed / invalid-PM2.5 / out-of-bounds / null-critical counts
# before and after filters, plus clean sample rows.
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E3_m4_data_quality_verification.py
```

**Epic 3 — Milestone 5: H3 verification**

```bash
# Verifies H3 format/validity/resolution and nearby-point same/neighbor behavior
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E3_m5_h3_verification.py
```

If a borough appears in raw traffic counts but shows `avg_pm25 = NULL` in
`workspace/02_live_data_exploration.ipynb`, reduce H3 granularity (for example,
resolution 8 -> 7) and re-run the business stream. This widens spatial matching
for sparse AQ coverage areas such as Bronx.

**Epic 3 — Milestone 6: H3 vs geometry benchmark**

```bash
# Runs sampled benchmark and writes report template at:
# workspace/benchmarks/E3_m6_h3_vs_geometry_report.md
docker exec -it -w /home/jovyan/work jupyter-pyspark python scripts/E3_m6_h3_vs_geometry_benchmark.py
```

Tip: after running, open Spark UI SQL tab and fill the report's TODO fields for
task/shuffle metrics on both benchmark queries.

**Epic 4 common failure modes (joins + enrichment):**

- **Late data mismatch / sparse AQ joins:** if a borough keeps `avg_pm25 = NULL` while raw traffic exists, keep AQ join at 15 minutes and reduce H3 granularity (for example, resolution 8 -> 7). Re-run the business stream from a clean business checkpoint namespace.
- **Weather join not contributing rows:** verify weather producer offsets are advancing and confirm `weather_event_ts` is non-null in raw weather sink (`s3a://raw-data/weather/`). Run `python scripts/E4_m4_weather_enrichment_verification.py`.
- **Static lookup sparsity:** rerun static lookup build after enough raw traffic seed coverage is present; then rerun business stream cells. Validate with `python scripts/E4_m5_static_lookup_verification.py`.
- **Checkpoint incompatibility after schema/state changes:** bump `CHECKPOINT_PATH` suffix or clear the exact checkpoint prefix before restart. For current config, clear `local.db.enriched_traffic_v4` if reusing that suffix.
- **Unexpected stream stalls/perf regression after static enrichment:** watch `[stream_metrics]` logs in the final notebook cell and Spark UI SQL tab; sustained growth in `trigger_execution_ms` suggests join/select plan pressure and should trigger a reduced test volume rerun.

**Stale checkpoint causing schema mismatch:**

```bash
# Option A (targeted): clear only the business stream checkpoint when
# local.db.enriched_traffic schema/state changes (current version shown)
docker exec -it minio mc rm -r --force myminio/business-data/checkpoints/local.db.enriched_traffic_v4/

# Option B (full reset): clear all checkpoints and restart from the top
docker exec -it minio mc rm -r --force myminio/raw-data/checkpoints/
docker exec -it minio mc rm -r --force myminio/refined-data/checkpoints/
docker exec -it minio mc rm -r --force myminio/business-data/checkpoints/

# Then restart the Jupyter kernel and re-run all cells
```

Note: deleting checkpoints causes replay from source offsets. If needed, also clean affected sink/table outputs to avoid duplicate downstream records.

**Full data reset (checkpoints + sinks + table):**

Use this when you want a truly clean replay after schema/checkpoint changes.

```bash
# 0) Stop streaming queries / notebook kernel first.

# 1) Clear all streaming checkpoints
docker exec -it minio mc rm -r --force myminio/raw-data/checkpoints/
docker exec -it minio mc rm -r --force myminio/refined-data/checkpoints/
docker exec -it minio mc rm -r --force myminio/business-data/checkpoints/

# 2) Clear raw sink outputs
docker exec -it minio mc rm -r --force myminio/raw-data/traffic/
docker exec -it minio mc rm -r --force myminio/raw-data/weather/
docker exec -it minio mc rm -r --force myminio/raw-data/openaq/
docker exec -it minio mc rm -r --force myminio/raw-data/purpleair/
docker exec -it minio mc rm -r --force myminio/raw-data/quarantine/

# 3) Clear refined sink outputs
docker exec -it minio mc rm -r --force myminio/refined-data/traffic/
docker exec -it minio mc rm -r --force myminio/refined-data/air_quality/
```

Then, in Spark (not shell), drop and recreate the Iceberg table via pipeline startup:

```python
spark.sql("DROP TABLE IF EXISTS local.db.enriched_traffic")
```

Optionally clean residual business table files if any remain:

```bash
docker exec -it minio mc rm -r --force myminio/business-data/db/enriched_traffic/
```

Finally restart producers and rerun `workspace/01_nyc_environmental_pipeline.ipynb` from the top.

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


### Dashboard Empty-Windows

Some panels can legitimately return no data for restrictive filter combinations.
This is especially common when:

- `congestion_zone = true`
- `truck_route = false`
- AQ-matched rows are sparse in the selected time window

When a panel is empty:

1. Switch one or both variables back to `All`.
2. Widen the dashboard time range.
3. For AQ panels, verify the underlying table still has AQ matches:

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(aq_pm25_ugm3) AS rows_with_pm25
FROM iceberg.db.enriched_traffic
WHERE traffic_event_ts BETWEEN from_iso8601_timestamp('${__from:date:iso}')
                          AND from_iso8601_timestamp('${__to:date:iso}')
```

4. For confounder-specific emptiness, inspect the slice directly:

```sql
SELECT
    is_congestion_zone,
    is_truck_route,
    COUNT(*) AS rows,
    COUNT(aq_pm25_ugm3) AS pm25_rows
FROM iceberg.db.enriched_traffic
WHERE traffic_event_ts BETWEEN from_iso8601_timestamp('${__from:date:iso}')
                          AND from_iso8601_timestamp('${__to:date:iso}')
GROUP BY is_congestion_zone, is_truck_route
ORDER BY rows DESC
```

### Grafana / Dashboard Issues

**Grafana Trino plugin install returns 404:**

Use the current plugin id:

```bash
docker exec -it grafana grafana cli plugins install trino-datasource
docker restart grafana
```

Do not use the older `grafana-trino-datasource` id.

**Grafana datasource test fails:**

Verify the datasource points at the Docker service hostname, not host loopback:

- URL: `http://trino:8080`
- Catalog: `iceberg`
- Schema: `db`

Then verify Trino directly:

```bash
docker exec -it trino trino --catalog iceberg --schema db --execute "SHOW TABLES"
```

**Grafana Explore works but dashboard panel query fails:**

Common causes:

- panel query still uses an old fixed-window example
- variable substitution syntax does not match the current dashboard variables
- the panel is filtering into an empty data slice

Use the current variable-safe filter pattern:

```sql
AND ('${congestion_zone}' = 'All' OR CAST(is_congestion_zone AS varchar) = '${congestion_zone}')
AND ('${truck_route}' = 'All' OR CAST(is_truck_route AS varchar) = '${truck_route}')
```

**Restrictive confounder combinations return no data:**

This can be expected. In local runs, combinations such as:

- `congestion_zone = true`
- `truck_route = false`

may legitimately produce no rows in the selected time window.

Fallback:

1. Switch one or both variables back to `All`.
2. Widen the dashboard time range.
3. Inspect the boolean slice counts in Trino using the query above.

**Geomap only shows Manhattan / Bronx-heavy points:**

This usually reflects the AQ-matched traffic distribution in
`iceberg.db.enriched_traffic`, not a rendering bug. Validate with:

```sql
SELECT
    traffic_borough,
    COUNT(*) AS rows_with_pm25,
    COUNT(DISTINCT CAST(traffic_lat AS varchar) || ',' || CAST(traffic_lon AS varchar)) AS distinct_points_with_pm25
FROM iceberg.db.enriched_traffic
WHERE aq_pm25_ugm3 IS NOT NULL
  AND traffic_lat IS NOT NULL
  AND traffic_lon IS NOT NULL
GROUP BY traffic_borough
ORDER BY rows_with_pm25 DESC
```

**AQ panels are empty even though traffic panels have data:**

Possible causes:

- AQ producers are failing upstream
- Docker container DNS cannot resolve OpenAQ / PurpleAir
- AQ-matched rows are sparse for the selected time/filter slice

Check DNS from the Jupyter container:

```bash
docker exec -it jupyter-pyspark python -c "import socket; print(socket.gethostbyname('api.openaq.org'))"
docker exec -it jupyter-pyspark python -c "import socket; print(socket.gethostbyname('api.purpleair.com'))"
```

If host DNS works but container DNS fails intermittently, restart Docker Desktop
and consider configuring stable DNS resolvers for Docker.
