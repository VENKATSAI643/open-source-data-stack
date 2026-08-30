-- assert_trip_physics.sql
-- Fails if any trips have physically impossible average speeds (> 150 mph)
-- or negative durations

{{ config(severity = 'warn') }}

WITH physics_check AS (
    SELECT
        vendor_id,
        pickup_datetime,
        trip_duration_min,
        trip_distance,
        avg_speed_mph
    FROM {{ ref('int_trips_enriched') }}
)
SELECT *
FROM physics_check
WHERE avg_speed_mph > 150
   OR trip_duration_min < 0
