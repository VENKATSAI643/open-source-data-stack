-- stg_taxi_trips.sql
-- Staging layer for Iceberg yellow_trips

SELECT
    assumeNotNull(pickup_datetime) AS pickup_datetime,
    assumeNotNull(dropoff_datetime) AS dropoff_datetime,
    passenger_count,
    trip_distance,
    assumeNotNull(pu_location_id) AS pickup_zone_id,
    assumeNotNull(do_location_id) AS dropoff_zone_id,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount,
    vendor_id
FROM {{ source('nyc_taxi_raw', 'yellow_trips') }}
WHERE pickup_datetime IS NOT NULL
  AND dropoff_datetime IS NOT NULL
  AND total_amount > 0
  AND trip_distance >= 0
