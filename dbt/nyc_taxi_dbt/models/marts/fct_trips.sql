-- fct_trips.sql
-- Grain: one row per trip
-- ClickHouse MergeTree engine: partitioned by month, sorted for common query patterns

{{
  config(
    engine   = 'MergeTree()',
    order_by = '(pickup_date, pickup_zone_id)',
    partition_by = 'toYYYYMM(pickup_datetime)'
  )
}}

SELECT
    -- Date dimension (for partitioning + joining dim_dates)
    toDate(pickup_datetime)     AS pickup_date,

    -- All enriched fields from intermediate
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    pickup_zone_id,
    dropoff_zone_id,
    pickup_zone_name,
    pickup_borough,
    dropoff_zone_name,
    dropoff_borough,
    payment_type,
    vendor_id,
    trip_duration_min,
    tip_pct,
    avg_speed_mph

FROM {{ ref('int_trips_enriched') }}
