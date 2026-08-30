-- dim_dates.sql
-- Date dimension generated from the range of dates in the fact table
-- ClickHouse numbers() function generates the date spine

{{
  config(
    engine   = 'MergeTree()',
    order_by = 'date_day'
  )
}}

SELECT
    toDate(min_date + toIntervalDay(number))  AS date_day,
    toYear(min_date + toIntervalDay(number))  AS year,
    toMonth(min_date + toIntervalDay(number)) AS month,
    toDayOfWeek(min_date + toIntervalDay(number)) AS day_of_week,
    toDayOfMonth(min_date + toIntervalDay(number)) AS day_of_month,
    toQuarter(min_date + toIntervalDay(number)) AS quarter,
    -- Weekend flag
    toDayOfWeek(min_date + toIntervalDay(number)) IN (6, 7) AS is_weekend,
    -- Month name
    dateName('month', min_date + toIntervalDay(number)) AS month_name,
    -- Day name
    dateName('weekday', min_date + toIntervalDay(number)) AS day_name

FROM (
    SELECT
        min(pickup_date) AS min_date,
        max(pickup_date) AS max_date,
        dateDiff('day', min(pickup_date), max(pickup_date)) AS total_days
    FROM {{ ref('fct_trips') }}
)
ARRAY JOIN range(total_days + 1) AS number
