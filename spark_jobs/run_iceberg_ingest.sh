#!/bin/bash
# Bridge script: invokes the Spark+Iceberg job from Project 3 with OpenLineage listener inside Docker
set -euo pipefail

# We mounted Project 3 into the Airflow container at /opt/project3
cd /opt/project3

# The Airflow container will have pyspark installed via _PIP_ADDITIONAL_REQUIREMENTS,
# so we can execute the Python job natively
PYTHONPATH=. python jobs/02_ingest_to_iceberg.py --recreate

# Refresh ClickHouse Iceberg table definition so dbt can read the newly written data
echo "Refreshing ClickHouse Iceberg table definition..."
curl -sS -X POST "http://clickhouse:8123/" -d "DROP TABLE IF EXISTS nyc_taxi_iceberg.yellow_trips;"
curl -sS -X POST "http://clickhouse:8123/" -d "CREATE TABLE nyc_taxi_iceberg.yellow_trips (vendor_id Nullable(Int32), pickup_datetime Nullable(DateTime64(6, 'UTC')), dropoff_datetime Nullable(DateTime64(6, 'UTC')), passenger_count Nullable(Int32), trip_distance Nullable(Float64), pu_location_id Nullable(Int32), do_location_id Nullable(Int32), payment_type Nullable(Int32), fare_amount Nullable(Float64), tip_amount Nullable(Float64), total_amount Nullable(Float64)) ENGINE = Iceberg('http://minio:9000/warehouse/taxi/trips', 'minioadmin', 'minioadmin123');"
echo "Refresh complete."
