with src as (
    select * from {{ source('raw', 'stations') }}
)

select
    cast(id as int64)                              as station_id,
    stationName                                    as station_name,
    cast(gegrLat as float64)                       as latitude,
    cast(gegrLon as float64)                       as longitude,
    `city.name`                                    as city_name,
    `city.commune.communeName`                     as commune_name,
    `city.commune.districtName`                    as district_name,
    `city.commune.provinceName`                    as province_name,
    addressStreet                                  as street_address,
    _ingested_at                                   as ingested_at
from src
