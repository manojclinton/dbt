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
        player_img

    from source

)

select * from renamed
order by player_name 
