{{ config(materialized='table') }}

select
    station_id,
    station_name,
    city_name,
    commune,
    district,
    province,
    street,
    latitude,
    longitude,
    case
        when latitude >= 53 then 'North'
        when latitude >= 51 then 'Central'
        else 'South'
    end as region,
    ingested_at
from {{ ref('stg_gios__stations') }}
