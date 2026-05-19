{{ config(
    materialized='incremental',
    unique_key=['sensor_id', 'measured_at']
) }}

with measurements as (
    select * from {{ ref('stg_gios__measurements') }}
    {% if is_incremental() %}
        where measured_at > (select coalesce(max(measured_at), timestamp '1900-01-01') from {{ this }})
    {% endif %}
),

stations as (
    select * from {{ ref('stg_gios__stations') }}
)

select
    m.sensor_id,
    m.station_id,
    s.station_name,
    s.city_name,
    s.province,
    s.latitude,
    s.longitude,
    m.pollutant_code,
    m.measured_at,
    m.measured_date,
    m.value,
    case m.pollutant_code
        when 'PM2.5' then m.value > {{ var('who_pm25_24h_limit') }}
        when 'PM10'  then m.value > {{ var('who_pm10_24h_limit') }}
        when 'NO2'   then m.value > {{ var('who_no2_24h_limit') }}
        else null
    end                            as exceeds_who_limit,
    current_timestamp              as dbt_updated_at
from measurements m
left join stations s using (station_id)
