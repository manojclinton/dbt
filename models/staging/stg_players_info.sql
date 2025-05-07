with 

source as (

    select * from {{ source('cricket_raw', 'players_info') }}

),

renamed as (

    select
        player_name,
        category,
        type,
        age,
        batting_style,
        bowling_style,
        team,
        player_url,
        player_img,
        REGEXP_EXTRACT(player_name, r'^[^ ]+\s+(.*)$') AS last_name

    from source

)

select * from renamed
order by player_name 
