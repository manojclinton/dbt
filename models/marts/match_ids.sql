{{
    config(
        materialized='view'
    )
}}

select
season,match_id,match_date,match_number,team1,team2
from {{ ref('int_match_info') }}
where season ='2025'
order by match_date, match_number desc