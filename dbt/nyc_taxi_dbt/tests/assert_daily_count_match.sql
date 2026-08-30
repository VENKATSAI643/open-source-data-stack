-- assert_daily_count_match.sql
-- Fails if any trips have more than 10 passengers

WITH passenger_check AS (
    SELECT
        vendor_id,
        passenger_count
    FROM {{ ref('int_trips_enriched') }}
)
SELECT *
FROM passenger_check
WHERE passenger_count > 10
