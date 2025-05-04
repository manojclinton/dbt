select season,
    match_id,
    datetime,
    temp_c,
    humidity_percent,
    pressure_hpa,
    cloudcover_percent,
    rain_mm,
    wind_m_s 
from {{ ref('stg_weather_info') }}

--gather required information only
  