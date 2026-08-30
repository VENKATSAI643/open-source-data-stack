# Phase 1: ClickHouse Validation Checks

## 1. SHOW TABLES FROM nyc_taxi_marts

**Command:**
```bash
curl "http://localhost:8123/?query=SHOW+TABLES+FROM+nyc_taxi_marts"
```

**Output:**
```
agg_daily_revenue
dim_dates
dim_zones
fct_trips
int_trips_enriched
stg_taxi_trips
stg_taxi_zones
taxi_zones
```

**Conclusion:**
All expected tables, views, and seeds for the marts layer have been successfully generated in the ClickHouse instance.

---

## 2. Row Count Verification for fct_trips

**Command:**
```bash
curl "http://localhost:8123/?query=SELECT+count()+FROM+nyc_taxi_marts.fct_trips"
```

**Output:**
```
7939988
```

**Conclusion:**
The primary `fct_trips` mart successfully populated with 7.93 million rows, confirming that the pipeline properly sourced the data from the Iceberg MinIO bucket.
