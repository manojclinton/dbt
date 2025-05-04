{{ config(
    materialized = 'view'
) }}

with base as (
  select
    match_id,
    inning_id,
    runs_total,
    runs_batter,
    batter,
    bowler,
    case when dismissal_kind is not null then 1 else 0 end as is_wicket
  from {{ ref('int_match_innings') }}
),

-- totals per inning
inning_totals as (
  select
    match_id,
    inning_id,
    sum(runs_total)    as inning_total,
    sum(is_wicket)     as total_wickets
  from base
  group by match_id, inning_id
),

-- top batter per inning
top_batter as (
  select
    match_id,
    inning_id,
    batter             as top_batter,
    sum(runs_batter)   as top_batter_runs,
    row_number() over(
      partition by match_id, inning_id
      order by sum(runs_batter) desc
    ) as rn
  from base
  group by match_id, inning_id, batter
  qualify rn = 1
),

-- top bowler per inning
top_bowler as (
  select
    match_id,
    inning_id,
    bowler             as top_bowler,
    sum(is_wicket)     as top_bowler_wickets,
    row_number() over(
      partition by match_id, inning_id
      order by sum(is_wicket) desc
    ) as rn
  from base
  where is_wicket = 1
  group by match_id, inning_id, bowler
  qualify rn = 1
),

match_meta as (
  select distinct
    season,
    match_id,
    match_number,
    playoff_stage,
    match_date,
    team1,
    team2,
    toss_winner,
    toss_decision,
    match_winner,
    margin,
    pom
  from {{ ref('int_match_info') }}
)


select
  m.season,
  m.match_id,
  m.match_number,
  m.playoff_stage,
  m.match_date,
  m.team1,
  m.team2,
  it.inning_id,
  it.inning_total,
  it.total_wickets,
  tb.top_batter,
  tb.top_batter_runs,
  tbo.top_bowler,
  tbo.top_bowler_wickets,
  m.toss_winner,
  m.toss_decision,
  m.match_winner,
  m.margin,
  m.pom 
from inning_totals it
join match_meta m
  using(match_id)
left join top_batter tb
  using(match_id, inning_id)
left join top_bowler tbo
  using(match_id, inning_id)
order by m.match_id, it.inning_id