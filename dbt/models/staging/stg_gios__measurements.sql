with src as (
    select * from {{ source('raw', 'measurements') }}
)

select
    cast(sensor_id as int64)                       as sensor_id,
    cast(station_id as int64)                      as station_id,
    upper(trim(param_code))                        as pollutant_code,
    timestamp(measured_at)                         as measured_at,
    date(timestamp(measured_at), 'Europe/Warsaw')  as measured_date,
    cast(value as float64)                         as value,
    _ingested_at                                   as ingested_at
from src
where value is not null
