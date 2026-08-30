-- dim_zones.sql
-- Dimension: one row per taxi zone (264 zones)

{{
  config(
    engine   = 'MergeTree()',
    order_by = 'zone_id'
  )
}}

SELECT
    zone_id,
    zone_name,
    borough,
    service_zone

FROM {{ ref('stg_taxi_zones') }}
