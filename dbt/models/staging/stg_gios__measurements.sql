-- One row per (sensor_id, measured_at). Sources can re-deliver the same
-- reading on the next ingest, so we dedupe on the natural key.
with src as (
    select * from {{ source('raw', 'measurements') }}
    where value is not null
),

deduped as (
    select
        *,
        row_number() over (
            partition by sensor_id, measured_at
            order by _ingested_at desc
        ) as _rn
    from src
)

select
    cast(sensor_id as bigint)         as sensor_id,
    cast(station_id as bigint)        as station_id,
    sensor_code,
    upper(trim(pollutant_code))       as pollutant_code,
    cast(measured_at as timestamp)    as measured_at,
    cast(measured_at as date)         as measured_date,
    cast(value as double)             as value,
    _ingested_at                      as ingested_at
from deduped
where _rn = 1
