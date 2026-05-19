-- Latest snapshot per station. Source parquet is append-only across
-- daily snapshots, so we keep the most recent row for each station_id.
with src as (
    select * from {{ source('raw', 'stations') }}
),

deduped as (
    select
        *,
        row_number() over (partition by station_id order by _ingested_at desc) as _rn
    from src
)

select
    cast(station_id as bigint)  as station_id,
    station_code,
    station_name,
    cast(latitude as double)    as latitude,
    cast(longitude as double)   as longitude,
    city_id,
    city_name,
    commune,
    district,
    province,
    street,
    _ingested_at                as ingested_at
from deduped
where _rn = 1
