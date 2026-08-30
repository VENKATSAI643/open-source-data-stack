-- assert_daily_agg_row_counts.sql
-- Singular test: daily aggregate trip count must match source row count per day
-- Catches silent data loss between fct_trips and agg_daily_revenue
-- Returns days where the counts diverge by more than 1% (tolerance for SummingMergeTree merges)

SELECT
    f.pickup_date,
    f.source_count,
    a.agg_count,
    abs(f.source_count - a.agg_count) AS count_diff,
    round(abs(f.source_count - a.agg_count) / f.source_count * 100, 2) AS pct_diff
FROM (
    SELECT
        pickup_date,
        count() AS source_count
    FROM {{ ref('fct_trips') }}
    GROUP BY pickup_date
) f
JOIN (
    SELECT
        pickup_date,
        sum(trip_count) AS agg_count    -- SummingMergeTree may have multiple parts
    FROM {{ ref('agg_daily_revenue') }}
    GROUP BY pickup_date
) a ON f.pickup_date = a.pickup_date
WHERE
    -- Fail if divergence is more than 1%
    abs(f.source_count - a.agg_count) / f.source_count > 0.01
