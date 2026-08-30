-- agg_daily_revenue.sql
-- Pre-aggregated mart for Superset dashboards (Dashboard 1 + Dashboard 4 comparison)
-- SummingMergeTree merges duplicate rows by summing metrics automatically

{{
  config(
    engine   = 'SummingMergeTree()',
    order_by = 'pickup_date'
  )
}}

SELECT
    pickup_date,
    count()                                 AS trip_count,
    sum(fare_amount)                        AS total_fare,
    sum(tip_amount)                         AS total_tips,
    sum(total_amount)                       AS total_revenue,
    round(avg(trip_distance), 4)            AS avg_trip_distance,
    round(avg(trip_duration_min), 2)        AS avg_trip_duration_min,
    round(avg(tip_pct), 2)                  AS avg_tip_pct

FROM {{ ref('fct_trips') }}
GROUP BY pickup_date
