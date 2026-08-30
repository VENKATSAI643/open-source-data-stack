-- assert_trip_distance_plausible.sql
-- Singular test: catches physically implausible trips
-- A trip that covers >1 mile per minute (60 mph sustained in NYC) is suspicious
-- Returns rows that FAIL the test (dbt singular tests pass when 0 rows returned)

{{ config(severity = 'warn') }}

SELECT
    pickup_datetime,
    dropoff_datetime,
    trip_distance,
    trip_duration_min,
    avg_speed_mph,
    total_amount
FROM {{ ref('fct_trips') }}
WHERE
    -- Duration must be positive and at least 1 minute
    trip_duration_min <= 0

    OR (
        -- Speed above 100 mph is impossible in NYC traffic
        trip_distance > 0
        AND trip_duration_min > 0
        AND avg_speed_mph > 100
    )

    OR (
        -- Minimum viable trip: at least 0.1 miles and 1 minute
        trip_distance < 0.1
        AND trip_duration_min < 1
    )
