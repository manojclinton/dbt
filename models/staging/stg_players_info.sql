with player_info_source as (

    select * from {{ source('cricket_raw', 'players_info') }}

),

player_info_renamed as (
    select
        name,
        DateofBirth as date_of_birth,
        gender,
        BattingStyle as batting_style,
        BowlingStyle as bowling_style,
        position

    from player_info_source

)

select * from player_info_renamed
