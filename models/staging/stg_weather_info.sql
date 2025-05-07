with 

source as (

    select * from {{ source('cricket_raw', 'weather_info') }}

),

renamed as (

    select
        season,
        match_id,
        city,
        match_num,
        venue,
        match_date,
        match_time,
        team1,
        team2,
        venue_id,
        latitude,
        longitude,
        datetime,
        temp_c,
        `humidity_%`       AS humidity_percent,
        pressure_hpa,
        `cloudcover_%`     AS cloudcover_percent,
        rain_mm,
        wind_m_s,
        total_views

    from source

)

select * from renamed
