/*
  Derived table: season_points_table
  - Computes IPL-style points per team per season, excluding playoffs
*/

{{ config(
    materialized = 'table'
) }}

with matches as (
  select
    match_id,
    season,
    team1,
    team2,
    match_winner,
    -- flag non-result matches
    case when lower(margin) = 'no result' then true else false end as no_result
  from {{ ref('int_match_info') }}
  where playoff_stage is null
),

-- explode to one row per team per match
team_matches as (
  select
    season,
    match_id,
    team1 as team,
    case 
      when no_result then 'no_result'
      when match_winner = team1 then 'win'
      else 'loss'
    end as result
  from matches
  union all
  select
    season,
    match_id,
    team2 as team,
    case 
      when no_result then 'no_result'
      when match_winner = team2 then 'win'
      else 'loss'
    end as result
  from matches
),

-- aggregate points per team per season
summary as (
  select
    season,
    team,
    count(*)                                             as matches_played,
    sum(case when result = 'win' then 1 else 0 end)      as wins,
    sum(case when result = 'loss' then 1 else 0 end)     as losses,
    sum(case when result = 'no_result' then 1 else 0 end) as no_results,
    sum(case when result = 'win' then 2                   
             when result = 'no_result' then 1
             else 0 end)                                 as points
  from team_matches
  group by season, team
),

new_pt as(

select
  season,
  team,
  matches_played,
  wins,
  losses,
  no_results,
  points,team_id,image_url
from summary s
join {{ ref('int_teams') }} tn on s.team=tn.team_name
)

select * from new_pt
order by season, points desc, wins desc, team
