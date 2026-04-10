FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root

# ── Replace Spark binaries with 4.0.2 to match the cluster ──────────────────
RUN wget -q https://archive.apache.org/dist/spark/spark-4.0.2/spark-4.0.2-bin-hadoop3.tgz \
    && tar -xzf spark-4.0.2-bin-hadoop3.tgz \
    && rm -rf ${SPARK_HOME} \
    && mv spark-4.0.2-bin-hadoop3 ${SPARK_HOME} \
    && rm spark-4.0.2-bin-hadoop3.tgz

# ── Install GDAL system library (required by geopandas/fiona for shapefiles) ─
RUN apt-get update --quiet && apt-get install --quiet -y libgdal-dev && rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

RUN pip install --quiet --no-cache-dir \
    \
    # ── Core Spark ────────────────────────────────────────────────────────────
    pyspark==4.0.2 \
    \
    # ── Kafka / Ingestion ─────────────────────────────────────────────────────
    # confluent-kafka is more robust than kafka-python for production streaming
    confluent-kafka \
    kafka-python \
    requests \
    python-dotenv \
    \
    # ── Spatial / Geospatial ──────────────────────────────────────────────────
    # apache-sedona: Spark spatial SQL (stream-to-stream spatial joins)
    apache-sedona \
    # shapely: geometry objects (points, polygons) used by Sedona
    shapely \
    # h3: Uber's hex-grid indexing for fast spatial bucketing
    h3 \
    # geopandas + fiona: reading shapefiles for truck routes & congestion zones
    geopandas \
    fiona \
    \
    # ── Apache Iceberg Data Lake ───────────────────────────────────────────────
    # pyiceberg[s3fs]: Python Iceberg client; s3fs extra lets it talk to MinIO
    "pyiceberg[s3fs,pyarrow]" \
    \
    # ── S3 / MinIO access ─────────────────────────────────────────────────────
    # boto3: AWS-compatible SDK, works with MinIO
    boto3 \
    # s3fs: filesystem-style access to S3/MinIO (used by pyiceberg & pandas)
    s3fs \
    \
    # ── Data manipulation ─────────────────────────────────────────────────────
    pandas \
    pyarrow \
    \
    # ── Visualization / Dashboard support ─────────────────────────────────────
    # plotly: for exploratory charts inside Jupyter before Grafana is wired up
    plotly