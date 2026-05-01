# Grafana Dashboard Setup

## Install the Trino Plugin

```bash
docker exec -it grafana grafana-cli plugins install grafana-trino-datasource
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

## Causal Analytics UI Configuration

### 1. Variables (The Toggles)

Create two Grafana Custom variables named `congestion_zone` and `truck_route`.

Use Key:Value pairs in the custom values field so Trino receives lowercase boolean strings correctly:

```text
True : true, False : false
```

This avoids the common casting issue where Trino treats mixed-case boolean-like strings as plain text instead of booleans.

Example dashboard filter pattern:

```sql
WHERE congestion_zone = CAST(${congestion_zone} AS BOOLEAN)
  AND truck_route = CAST(${truck_route} AS BOOLEAN)
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

- Residential Streets: `truck_route = false`, `congestion_zone = false`
- Outer Highways: `truck_route = true`, `congestion_zone = false`
- Manhattan Gridlock: `truck_route = true`, `congestion_zone = true`

That structure makes the causal story easier to inspect directly in the UI.