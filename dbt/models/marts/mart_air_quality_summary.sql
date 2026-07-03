-- Gold/mart layer: dashboard-ready table, one row per city/parameter/day.
-- Accumulates naturally as the Airflow DAG runs daily.
-- Trend chart uses all rows; pollutant breakdown filters to max(measured_at).

with daily as (
    select * from {{ ref('int_air_quality_daily_avg') }}
),

flagged as (
    select
        *,
        case
            when parameter = 'pm25' and avg_value > 60   then true
            when parameter = 'pm10' and avg_value > 100  then true
            when parameter = 'no2'  and avg_value > 80   then true
            when parameter = 'so2'  and avg_value > 80   then true
            when parameter = 'co'   and avg_value > 4000 then true
            else false
        end as is_unhealthy
    from daily
)

select * from flagged
order by measured_at desc, city, parameter