{{
  config(
    materialized = 'view',
    engine       = 'View()'
  )
}}

SELECT
    t.vendor_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.pickup_zone_id,
    t.dropoff_zone_id,
    t.payment_type,
    t.fare_amount,
    t.tip_amount,
    t.total_amount,

    -- Derived fields
    dateDiff('minute', t.pickup_datetime, t.dropoff_datetime)  AS trip_duration_min,
    CASE WHEN dateDiff('minute', t.pickup_datetime, t.dropoff_datetime) > 0
         THEN t.trip_distance / (dateDiff('minute', t.pickup_datetime, t.dropoff_datetime) / 60.0)
         ELSE NULL
    END                                                         AS avg_speed_mph,
    CASE WHEN t.fare_amount > 0
         THEN (t.tip_amount / t.fare_amount) * 100
         ELSE 0
    END                                                         AS tip_pct,

    -- Zone enrichment
    pz.zone_name   AS pickup_zone_name,
    pz.borough     AS pickup_borough,
    dz.zone_name   AS dropoff_zone_name,
    dz.borough     AS dropoff_borough

FROM {{ ref('stg_taxi_trips') }} t
LEFT JOIN {{ ref('stg_taxi_zones') }} pz ON t.pickup_zone_id = pz.zone_id
LEFT JOIN {{ ref('stg_taxi_zones') }} dz ON t.dropoff_zone_id = dz.zone_id
