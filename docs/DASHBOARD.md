# Grafana Dashboard Setup

## Install the Trino Plugin

```bash
docker exec -it grafana grafana cli plugins install trino-datasource
docker restart grafana
```

## Add Trino as a Data Source

In Grafana at http://localhost:3000, go to Connections -> Data Sources -> Add new data source -> Trino.

| Setting | Value |
|---|---|
| Host | `trino:8080` |
| Catalog | `iceberg` |
| Schema | `db` |
| Username | `admin` |

Click Save & Test.

## Verify Trino Can Query the Table

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

## Example Panel Queries

**General Feedback-Loop Query (dashboard variable aware):**

```sql
SELECT
    date_trunc('minute', traffic_event_ts) AS time,
    AVG(traffic_speed) AS avg_speed,
    AVG(aq_pm25_ugm3) AS avg_pm25
FROM iceberg.db.enriched_traffic
WHERE traffic_event_ts BETWEEN from_iso8601_timestamp('${__from:date:iso}')
                          AND from_iso8601_timestamp('${__to:date:iso}')
  AND ('${congestion_zone}' = 'All' OR CAST(is_in_congestion_zone AS varchar) = '${congestion_zone}')
  AND ('${truck_route}' = 'All' OR CAST(is_near_truck_route AS varchar) = '${truck_route}')
GROUP BY 1
ORDER BY 1
```

This query is the recommended baseline because it:

- uses the Grafana dashboard time picker instead of a fixed trailing interval
- keeps panel output aligned with the pipeline's 15-minute join cadence
- supports dashboard-wide boolean filtering without Trino cast errors

**Borough Average Speed (Time Series):**

```sql
SELECT
    date_trunc('minute', traffic_event_ts) AS time,
    traffic_borough,
    AVG(traffic_speed) AS avg_speed
FROM iceberg.db.enriched_traffic
WHERE traffic_event_ts BETWEEN from_iso8601_timestamp('${__from:date:iso}')
                          AND from_iso8601_timestamp('${__to:date:iso}')
  AND ('${congestion_zone}' = 'All' OR CAST(is_in_congestion_zone AS varchar) = '${congestion_zone}')
  AND ('${truck_route}' = 'All' OR CAST(is_near_truck_route AS varchar) = '${truck_route}')
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
  AND traffic_event_ts BETWEEN from_iso8601_timestamp('${__from:date:iso}')
                          AND from_iso8601_timestamp('${__to:date:iso}')
  AND ('${congestion_zone}' = 'All' OR CAST(is_in_congestion_zone AS varchar) = '${congestion_zone}')
  AND ('${truck_route}' = 'All' OR CAST(is_near_truck_route AS varchar) = '${truck_route}')
GROUP BY 1
ORDER BY 1
```

**Residential Streets: PM2.5 vs Traffic Speed Correlation (Line Graph):**

```sql
SELECT
  date_trunc('minute', traffic_event_ts) AS time,
  CAST(AVG(traffic_speed) AS DOUBLE) AS avg_speed,
  CAST(AVG(aq_pm25_ugm3) AS DOUBLE) AS avg_pm25
FROM iceberg.db.enriched_traffic
WHERE traffic_event_ts BETWEEN from_iso8601_timestamp('${__from:date:iso}')
                          AND from_iso8601_timestamp('${__to:date:iso}')
  AND aq_pm25_ugm3 IS NOT NULL
  AND is_in_congestion_zone = false
  AND is_near_truck_route = false
GROUP BY date_trunc('minute', traffic_event_ts)
ORDER BY time ASC
```

**Outer Highways: PM2.5 vs Traffic Speed Correlation (Line Graph):**

```sql
SELECT
  date_trunc('minute', traffic_event_ts) AS time,
  CAST(AVG(traffic_speed) AS DOUBLE) AS avg_speed,
  CAST(AVG(aq_pm25_ugm3) AS DOUBLE) AS avg_pm25
FROM iceberg.db.enriched_traffic
WHERE traffic_event_ts BETWEEN from_iso8601_timestamp('${__from:date:iso}')
                          AND from_iso8601_timestamp('${__to:date:iso}')
  AND aq_pm25_ugm3 IS NOT NULL
  AND is_in_congestion_zone = false
  AND is_near_truck_route = true
GROUP BY date_trunc('minute', traffic_event_ts)
ORDER BY time ASC
```

**AQ Join Coverage Over Time (Hourly, Bar Graph):**

```sql
SELECT
  date_trunc('hour', traffic_event_ts) AS time,
  CAST(
    100.0 * SUM(CASE WHEN aq_pm25_ugm3 IS NOT NULL THEN 1 ELSE 0 END)
    / CAST(COUNT(*) AS DOUBLE)
    AS DOUBLE
  ) AS aq_join_coverage_percent
FROM iceberg.db.enriched_traffic
WHERE $__timeFilter(traffic_event_ts)
GROUP BY 1
ORDER BY 1 ASC
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

## Causal Analytics UI Configuration

### 1. Variables (The Toggles)

Create two Grafana Custom variables named `congestion_zone` and `truck_route`.

Use these custom values so the dashboard can either show all data or apply a
specific boolean filter:

```text
All,false,true
```

Set `All` as the default value for both variables. This keeps the dashboard
populated on first load and avoids accidentally filtering into empty slices when
some confounder combinations are sparse in the current time range.

Example dashboard filter pattern:

```sql
WHERE ('${congestion_zone}' = 'All' OR CAST(is_in_congestion_zone AS varchar) = '${congestion_zone}')
  AND ('${truck_route}' = 'All' OR CAST(is_near_truck_route AS varchar) = '${truck_route}')
```

### 2. Dual-Axis Time Series

Use Grafana panel Overrides to split speed and pollution into separate axes.

Configuration pattern:

1. Open the Time series panel.
2. Go to Overrides.
3. Add an override for Fields with name -> `avg_pm25`.
4. Set Axis -> Placement -> Right.
5. Leave `avg_speed` on the default left axis.

This keeps traffic speed on the left and PM2.5 on the right so the two signals are readable even when their numeric ranges differ significantly.

### 3. Timezone Lock

The Iceberg data lake stores timestamps in UTC, which is safe for the floating NYC DOT event timestamps and downstream joins.

Lock the dashboard timezone to `America/New_York` in Dashboard General Settings so the chart labels and time windows display correctly for local analysis.

## Recommended Dashboard Pattern

Use one comparison panel per visual state:

- Residential Streets: `is_near_truck_route = false`, `is_in_congestion_zone = false`
- Outer Highways: `is_near_truck_route = true`, `is_in_congestion_zone = false`
- Manhattan Gridlock: `is_near_truck_route = true`, `is_in_congestion_zone = true`

That structure makes the causal story easier to inspect directly in the UI.


## Geomap Panels

Use two complementary Geomap panels:

- a point-level hotspot map showing AQ-matched traffic points
- a borough-summary map showing one marker per borough

### Panel 1: AQ-Matched Traffic Points

Use this query for the detailed hotspot map:

```sql
SELECT
    traffic_event_ts AS time,
    traffic_borough,
    traffic_lat AS latitude,
    traffic_lon AS longitude,
    ROUND(AVG(traffic_speed), 2) AS avg_speed,
    ROUND(AVG(aq_pm25_ugm3), 2) AS avg_pm25,
    COUNT(*) AS point_count
FROM iceberg.db.enriched_traffic
WHERE traffic_event_ts BETWEEN from_iso8601_timestamp('${__from:date:iso}')
                          AND from_iso8601_timestamp('${__to:date:iso}')
  AND aq_pm25_ugm3 IS NOT NULL
  AND traffic_lat IS NOT NULL
  AND traffic_lon IS NOT NULL
  AND ('${congestion_zone}' = 'All' OR CAST(is_in_congestion_zone AS varchar) = '${congestion_zone}')
  AND ('${truck_route}' = 'All' OR CAST(is_near_truck_route AS varchar) = '${truck_route}')
GROUP BY 1, 2, 3, 4
ORDER BY time
LIMIT 1000
```

Recommended Grafana Geomap configuration:

- Visualization: `Geomap`
- Location mode: `Coordinates`
- Latitude field: `latitude`
- Longitude field: `longitude`
- Marker color: `avg_pm25`
- Marker size: `point_count`
- Tooltip: enabled
- Initial map center:
  - Latitude: `40.7128`
  - Longitude: `-74.0060`
  - Zoom: `10`


### Panel 2: Borough AQ Summary

Use this query for one borough-level marker per borough:

```sql
SELECT
    traffic_borough,
    ROUND(AVG(traffic_lat), 6) AS latitude,
    ROUND(AVG(traffic_lon), 6) AS longitude,
    COUNT(*) AS rows,
    ROUND(AVG(traffic_speed), 2) AS avg_speed,
    ROUND(AVG(aq_pm25_ugm3), 2) AS avg_pm25
FROM iceberg.db.enriched_traffic
WHERE aq_pm25_ugm3 IS NOT NULL
  AND traffic_lat IS NOT NULL
  AND traffic_lon IS NOT NULL
  AND traffic_event_ts BETWEEN from_iso8601_timestamp('${__from:date:iso}')
                          AND from_iso8601_timestamp('${__to:date:iso}')
  AND ('${congestion_zone}' = 'All' OR CAST(is_in_congestion_zone AS varchar) = '${congestion_zone}')
  AND ('${truck_route}' = 'All' OR CAST(is_near_truck_route AS varchar) = '${truck_route}')
GROUP BY traffic_borough
```

Recommended Grafana Geomap configuration:

- Visualization: `Geomap`
- Location mode: `Coordinates`
- Latitude field: `latitude`
- Longitude field: `longitude`
- Marker size: `rows`
- Marker color:
  - either fixed color for a clean borough summary
  - or `avg_speed` if you want borough markers colored by average speed
- Tooltip: enabled
- Initial map center:
  - Latitude: `40.7128`
  - Longitude: `-74.0060`
  - Zoom: `10`

The point-level map can look sparse or concentrated because AQ-matched joins
  happen at a limited set of traffic coordinates.

## Epic 6 Milestone 6: End-to-End Runbook

### Bring-Up Order

1. Start the stack:

```bash
docker compose up -d --build
```

2. Confirm core services are healthy:

```bash
docker compose ps
```

3. Initialize static datasets if needed:

See the `Static Data Onboarding` section in [README.md](nyc-traffic-emissions-pipeline/README.md).


4. Start the data producers:

```bash
docker exec -it -w /home/jovyan/work jupyter-pyspark bash
python run_all.py
```

5. Open Jupyter and run the streaming notebook:

- URL: `http://localhost:8888?token=bigdata`
- Open `01_nyc_environmental_pipeline.ipynb`
- Run all cells and keep the final streaming cell active

6. Verify the business table exists and has rows:

```bash
docker exec -it trino trino --catalog iceberg --schema db --execute "SELECT COUNT(*) FROM enriched_traffic"
```

7. Open Grafana:

- URL: `http://localhost:3000`
- Login: `admin / admin`

8. Verify the Trino datasource:

- `Connections -> Data sources -> Trino`
- Click `Save & Test`
- In `Explore`, run:

```sql
SELECT *
FROM iceberg.db.enriched_traffic
LIMIT 5
```

9. Build or open the dashboard panels:

- General Feedback-Loop Query (Time Series) \[Optional\]
- Residential Streets: PM2.5 vs Traffic Speed Correlation
- Outer Highways: PM2.5 vs Traffic Speed Correlation
- AQ join coverage over time (hourly)
- Borough Average Speed (Time Series)
- AQ-Matched Traffic Points Geomap
- Borough AQ Summary Geomap

10. Set dashboard defaults:

- time range: `Last 24 hours`
- `congestion_zone = All`
- `truck_route = All`
- dashboard timezone: `America/New_York`
- refresh: start at `1m`
