# Formal Report

## Abstract

This project investigates a familiar urban hypothesis: traffic worsens air pollution. The challenge is not the hypothesis itself, but the noise surrounding it. Weather fluctuations, diesel freight corridors, building emissions, and sparse sensor coverage can all obscure the effect of passenger car congestion. To address this problem, we built a real-time distributed data pipeline that joins live NYC traffic observations with air quality, weather, and geospatial context, then filters the result through H3 indexing, a sliding temporal window, and spatial classification with Apache Sedona.

The resulting analytical layer isolates passenger-car-driven pollution effects from major confounders. The dashboard does not claim that every congestion event produces the same pollutant response. Instead, it reveals when traffic slowdown aligns with PM2.5 spikes, when truck routes dominate the air quality signal, and when real-world sensor deserts prevent a clean measurement altogether.

## Run Snapshot (Current Local Validation)

Reference notebooks:

- `workspace/01_nyc_environmental_pipeline.ipynb`
- `workspace/02_live_data_exploration.ipynb`

Key metrics from the latest observed run:

- Business table rows: ~13.5M (`total_enriched_records` about 13,538,254)
- Boroughs covered: 5
- Unique H3 cells: 50
- AQ join coverage: ~96.6%
- Weather join coverage: ~99.8%
- Time span in business table:
  - `pipeline_start`: 2026-05-01 14:38:00
  - `pipeline_latest`: 2026-05-07 18:28:11
- Borough AQ non-null coverage:
  - Manhattan: 98.85%
  - Bronx: 97.88%
  - Brooklyn: 97.63%
  - Queens: 92.71%
  - Staten Island: 78.44%

Interpretation note: this snapshot is environment- and time-window-dependent. Recompute after any major rerun, checkpoint reset, or parameter change (window, watermark, H3 resolution).

## Introduction

Urban air quality studies often rely on historical and aggregated datasets that are too coarse to expose short-lived traffic effects. This project adopts a streaming architecture so the analysis occurs close to the event itself. Traffic speeds from NYC DOT, PM2.5 measurements from OpenAQ and PurpleAir, and weather observations from OpenWeatherMap are ingested continuously and correlated in near real time.

The central thesis is straightforward: traffic worsens pollution, but the signal is noisy. To measure the environmental cost of congestion more faithfully, the pipeline strips away confounding variables such as truck traffic, congestion zones, and building-related emissions. This produces a cleaner analytical surface for assessing passenger-car impact.

## System Architecture

The pipeline is organized as a medallion-style streaming system:

1. Producers poll external APIs and publish JSON records to Redpanda topics.
2. Spark Structured Streaming consumes the topics and applies event-time logic, watermarking, and H3 spatial indexing.
3. Spatial enrichment joins live traffic with static polygon layers representing truck routes, congestion zones, and building-energy geographies.
4. Enriched records are written to MinIO-backed Apache Iceberg tables.
5. Trino queries the Iceberg data to serve Grafana dashboards and analytical summaries.

This architecture is designed to preserve the raw source signals while creating an analysis-ready layer that supports both exploratory and causal-style visual comparisons.

## Methodology

The production notebook currently aligns traffic, AQ, and weather streams with a 15-minute event-time window and 15-minute watermarks. The spatial join key is H3 resolution 7 (updated from earlier coarser settings during calibration). This resolution was selected to improve borough-level AQ coverage while keeping join state manageable in local streaming runs.

Static geospatial context is precomputed into an H3 lookup table (congestion zone, truck route proximity, high-emission-building proximity), then joined into the live traffic stream before AQ and weather enrichment. This two-step pattern reduces expensive per-row geometry checks inside the main stream-to-stream join path.

Weather is retained as a confounder context stream and normalized with null-safe casting (`TRY_CAST`) to avoid malformed payload failures. The resulting business stream preserves unmatched rows via explicit diagnostic flags (`has_aq_match`, `has_weather_match`) instead of dropping records at join boundaries.

## Analytical Findings

Current notebook outputs show five stable themes:

### 1. Pipeline coverage is high but not uniform

The business table contains approximately 13.5M enriched rows across 5 boroughs and 50 H3 cells. Join coverage is strong overall (`aq_join_pct` about 96.6%, `weather_join_pct` about 99.8%), but borough-level AQ non-null coverage remains uneven:

- Manhattan: 98.85%
- Bronx: 97.88%
- Brooklyn: 97.63%
- Queens: 92.71%
- Staten Island: 78.44%

The key interpretation is that aggregate reliability is good, but local confidence should be borough-sensitive.

### 2. Truck-route context contributes the strongest PM2.5 uplift

Confounder segmentation indicates the largest positive PM2.5 uplift in truck-route segments:

- `is_in_congestion_zone = false`, `is_near_truck_route = true`: avg PM2.5 about 7.26 (uplift about +1.95 vs baseline)
- Baseline (`false`, `false`): avg PM2.5 about 5.31
- `true`, `false`: avg PM2.5 about 5.56 (small uplift, low sample count)

This supports the interpretation that freight corridors are a dominant pollution context in this dataset.

### 3. Speed-only aggregation is not a causal estimator

Speed-bucket summaries in the current run show higher PM2.5 in faster buckets (for example, free-flow about 8.87 vs gridlock about 5.16). This does not imply congestion is clean; it indicates that speed-only grouping is confounded by location/time composition and sensor distribution. The notebook’s confounder controls and borough-hour views are required for interpretation.

### 4. Temporal patterning is concentrated in afternoon windows

Hour-of-day outputs show meaningful variability, with elevated PM2.5 in selected afternoon periods (for example, hour 14 about 10.45 and hour 15 about 10.11 in the sampled run), while overnight values remain lower and flatter.

### 5. Pollution hotspots are persistent at selected H3 cells

Top hotspot persistence analysis (hourly top-10 PM2.5 membership) shows repeated reappearance of the same H3 cells, with leading cells appearing roughly 18-27 hourly windows over the analysis interval. This suggests repeated localized exposure zones rather than one-off spikes.

## Known Limitations & Future Optimizations

The primary technical limitation remains state growth in stream-to-stream joins under H3 + temporal window matching. This can inflate intermediate row counts and increase local runtime/storage costs, especially when AQ sensor density and traffic density overlap unevenly.

This affects efficiency more than metric correctness, but it can degrade iteration speed and complicate reproducibility of ad hoc exploratory slices.

Recommended optimization path:

1. Keep current H3=7 as default for coverage.
2. Evaluate narrower join windows per use case (for example, AQ-specific sensitivity runs).
3. Introduce optional pre-aggregation only with carefully tuned watermark strategy to avoid late-window drop behavior.
4. Add partition-aware retention/compaction in Iceberg to control table growth over long-running sessions.

Operational limitations that still matter:

- OpenAQ/PurpleAir geographic sparsity is borough-dependent.
- Weather and AQ payload quality can vary by provider interval.
- Local single-cluster deployment prioritizes development velocity over horizontal scale.

## Conclusion

This project shows that real-time urban traffic analysis is feasible when the pipeline is designed to separate signal from confounding context. By joining traffic, air quality, weather, and geospatial metadata in a streaming architecture, the system surfaces the environmental consequences of congestion in a way that static datasets cannot.

The current findings reinforce that interpretation quality depends on context controls. Aggregate coverage is strong, but borough and corridor heterogeneity materially affects conclusions. In practice, truck-route context and hotspot persistence provide clearer explanatory signal than speed-only averages.

The hub-and-spoke documentation structure remains appropriate: setup, dashboard usage, troubleshooting, and this formal report each address a distinct layer of operational and analytical decision-making.