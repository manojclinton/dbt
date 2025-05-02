with stadium_coords as (

    select * from {{ source('cricket_raw', 'stadium_coordinates') }}

),

cte as (

    select
        venue_id,
        city_cleaned,
        venue_cleaned,
        latitude,
        longitude

    from stadium_coords

)

select * from cte 
order by venue_id
