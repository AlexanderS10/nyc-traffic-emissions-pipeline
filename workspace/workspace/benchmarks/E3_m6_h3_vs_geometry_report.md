## Epic 3 Milestone 6 Benchmark Report

Generated: `2026-05-05T19:10:43.265467+00:00`

### Benchmark Config
- traffic_path: `s3a://refined-data/traffic/`
- aq_path: `s3a://refined-data/air_quality/`
- hours_back: `12`
- traffic_sample: `30000`
- aq_sample: `30000`
- time_window_minutes: `15`
- distance_threshold_m: `500.0`
- bucket_seconds: `300`

### Results
- H3 join elapsed_s: `0.280` (rows=`19118`)
- Geometry-style join elapsed_s: `2.596` (rows=`21493`)
- Relative speedup (geometry/h3): `9.28x`

### Spark UI Metrics (manual capture)
Capture from Spark UI SQL tab for each benchmark query:

- H3 query
  - total_duration_s: `TODO`
  - total_tasks: `TODO`
  - shuffle_read_bytes: `TODO`
  - shuffle_write_bytes: `TODO`
- Geometry-style query
  - total_duration_s: `TODO`
  - total_tasks: `TODO`
  - shuffle_read_bytes: `TODO`
  - shuffle_write_bytes: `TODO`

### Interpretation
- If H3 query is materially faster and has lower shuffle, keep H3 strategy.
- If AQ match quality drops too much, tune H3 resolution or time window and rerun.
- Current local tuning decision: keep H3 strategy but use coarser resolution 7 in the pipeline to reduce borough-level AQ null gaps (Bronx `avg_pm25` was null at finer settings).

### H3 Resolution Tuning Note
- Increase resolution (smaller cells) when false spatial matches are too high.
- Decrease resolution (larger cells) when match sparsity is too high or AQ coverage drops.
- Re-evaluate whenever sensor density or target join radius changes.
- Latest observed trade-off in this repo: resolution 7 improves AQ coverage continuity across boroughs while preserving acceptable benchmark speedup.
