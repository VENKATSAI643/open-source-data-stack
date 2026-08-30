-- stg_taxi_zones.sql
-- Staging layer: 1:1 with source zones lookup table
-- Rename columns to snake_case to match downstream refs

SELECT
    LocationID   AS zone_id,
    Borough      AS borough,
    Zone         AS zone_name,
    service_zone

FROM {{ ref('taxi_zones') }}

WHERE LocationID IS NOT NULL
