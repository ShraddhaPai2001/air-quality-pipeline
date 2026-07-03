-- Silver/intermediate layer: one row per city/parameter/ingestion_date.
-- Groups by ingestion_date (the date the Airflow DAG ran) so that each daily
-- pipeline run contributes exactly one date slice to the trend chart.
-- This accumulates naturally over time as the DAG runs daily.

with base as (
    select * from {{ ref('stg_air_quality_measurements') }}
),

daily_avg as (
    select
        city,
        parameter,
        ingestion_date                as measured_at,   -- the date Airflow ran
        avg(value)                    as avg_value,
        min(value)                    as min_value,
        max(value)                    as max_value,
        count(*)                      as num_readings
    from base
    group by city, parameter, ingestion_date
)

select * from daily_avg