# Formal Report

## Abstract

This project investigates a familiar urban hypothesis: traffic worsens air pollution. The challenge is not the hypothesis itself, but the noise surrounding it. Weather fluctuations, diesel freight corridors, building emissions, and sparse sensor coverage can all obscure the effect of passenger car congestion. To address this problem, we built a real-time distributed data pipeline that joins live NYC traffic observations with air quality, weather, and geospatial context, then filters the result through H3 indexing, a sliding temporal window, and spatial classification with Apache Sedona.

The resulting analytical layer isolates passenger-car-driven pollution effects from major confounders. The dashboard does not claim that every congestion event produces the same pollutant response. Instead, it reveals when traffic slowdown aligns with PM2.5 spikes, when truck routes dominate the air quality signal, and when real-world sensor deserts prevent a clean measurement altogether.

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

The pipeline uses a 15-minute sliding window to align traffic and air quality records by space and time. H3 resolution 5 provides a coarse but practical spatial key that improves join hit rates in a city where real-time sensor coverage is uneven. Apache Sedona adds a second layer of spatial context by tagging traffic points relative to freight corridors and congestion geofences.

Weather observations are incorporated as an additional confounder control. Although weather does not directly explain every pollution spike, it is necessary context because wind, precipitation, and atmospheric stability alter pollutant dispersion. The overall result is a multi-stream enrichment workflow aimed at distinguishing genuine traffic effects from nearby environmental noise.

## Analytical Findings

The dashboard revealed three consistent visual states.

### 1. Residential Streets: Truck Route = False, Congestion = False

This view provides the clearest evidence of the feedback loop between speed and pollution. Baseline PM2.5 is low, around 3.0, but when traffic speed drops, PM2.5 rises almost immediately. The pattern is visible because the street environment is relatively quiet: there is less freight traffic, fewer geofenced congestion effects, and less background emission pressure from adjacent commercial corridors.

### 2. Outer Highways: Truck Route = True, Congestion = False

This state demonstrates diesel impact. Baseline PM2.5 is roughly double the residential baseline, often 6.0 or higher, which masks ordinary passenger car emissions. The highway signal is not simply more traffic; it is a different emissions regime. Heavy vehicles dominate the pollution profile, so the relationship between car speed and PM2.5 becomes harder to isolate without the truck-route flag.

### 3. Manhattan Gridlock: Both = True

This is the strongest example of Big Data veracity issues. Speeds are abysmal, typically 7 to 13 mph, but PM2.5 readings sometimes disappear entirely. The absence is not a modeling failure; it is a real sensor-coverage problem. In dense urban cores, the dashboard exposes sensor deserts where the signal is too sparse to support a stable air-quality conclusion, even though the traffic condition itself is unmistakable.

## Known Limitations & Future Optimizations

The largest technical limitation is the cross-product effect created by the H3 spatial join combined with the 15-minute sliding window. In practice, this can inflate the intermediate result set into a Cartesian-like expansion with more than 4.2 million rows for a short time window. That increases storage footprint and makes the streaming join heavier than necessary.

Importantly, this does not invalidate the downstream analytical averages. The Trino-side `AVG()` aggregations remain accurate because the duplicated records are still consistently represented in the final grouped computation. The issue is efficiency, not correctness.

The recommended production optimization is to pre-aggregate each stream before the join. For example, traffic and air-quality inputs could be reduced into tumbling 1-minute windows, then joined at that lower cardinality. That would materially reduce state size, lower storage cost, and improve streaming performance while preserving the analytical signal needed for dashboard-level comparisons.

Other practical limitations remain. OpenAQ coverage is sparse in parts of NYC, PurpleAir timestamps are approximated from ingestion time, and the system currently favors local development simplicity over scale-out parallelism.

## Conclusion

This project shows that real-time urban traffic analysis is feasible when the pipeline is designed to separate signal from confounding context. By joining traffic, air quality, weather, and geospatial metadata in a streaming architecture, the system surfaces the environmental consequences of congestion in a way that static datasets cannot.

The main takeaway is not simply that traffic increases pollution. It is that the effect is conditional: residential streets, freight corridors, and gridlocked Manhattan blocks behave differently, and each requires a separate analytical frame. The hub-and-spoke documentation structure mirrors that conclusion by separating operational setup, dashboard usage, troubleshooting, and formal findings into focused companion documents.