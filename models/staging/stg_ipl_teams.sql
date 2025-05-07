with 

source as (

    select * from {{ source('cricket_raw', 'ipl_teams') }}

),

renamed as (

    select
        team_id,
        team,
        image_url

    from source

)

select * from renamed
