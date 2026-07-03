-- Bronze/staging layer: clean raw measurements from OpenAQ daily ingestion.
-- Reads from raw_air_quality_measurements (populated by Airflow DAG each daily run).

with source as (
    select * from {{ source('raw', 'raw_air_quality_measurements') }}
),

cleaned as (
    select
        id,
        lower(trim(city))      as city,
        location_id,
        location_name,
        latitude,
        longitude,
        lower(trim(parameter)) as parameter,
        value::double precision as value,
        unit,
        measured_at::timestamp as measured_at,
        ingestion_date::date   as ingestion_date,
        loaded_at
    from source
    where value is not null
      and parameter is not null
)

select * from cleaned