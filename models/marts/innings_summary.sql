{{ config(materialized='view') }}

with base as (
  select
    match_id,
    inning_id,
    batter,
    bowler,
    runs_batter,
    runs_total,
    case when dismissal_kind is not null then 1 else 0 end as is_wicket,

    -- every delivery counts as one ball faced by batter
    1 as balls_faced,

    -- flag legal deliveries (exclude wides/no-balls)
    case
      when extra_type in ('wides','noballs') then 0
      else 1
    end as legal_ball

  from {{ ref('int_match_innings') }}
),

-- 1) inning totals (unchanged)
inning_totals as (
  select
    match_id,
    inning_id,
    sum(runs_total) as inning_total,
    sum(is_wicket)  as total_wickets
  from base
  group by match_id, inning_id
),

-- 2) batter aggregates
batter_agg as (
  select
    match_id,
    inning_id,
    batter,
    sum(runs_batter)  as total_runs,
    sum(balls_faced)  as total_balls
  from base
  group by match_id, inning_id, batter
),

-- 3) rank batters (top 2)
batters_ranked as (
  select
    *,
    row_number() over(
      partition by match_id, inning_id
      order by total_runs desc, total_balls asc
    ) as rn
  from batter_agg
),

-- 4) bowler aggregates
bowler_agg as (
  select
    match_id,
    inning_id,
    bowler,
    sum(runs_total)    as runs_conceded,
    sum(legal_ball)    as balls_bowled,
    sum(is_wicket)     as total_wickets
  from base
  group by match_id, inning_id, bowler
),

-- 5) rank bowlers (top 2 with your tiebreaker)
bowlers_ranked as (
  select
    match_id,
    inning_id,
    bowler,
    runs_conceded,
    balls_bowled,
    total_wickets,
    row_number() over(
      partition by match_id, inning_id
      order by
        total_wickets desc,
        runs_conceded asc,
        balls_bowled asc
    ) as rn
  from bowler_agg
),

-- 6) match metadata
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

  -- top batter
  tb.batter        as top_batter,
  tb.total_runs    as top_batter_runs,
  tb.total_balls   as top_batter_balls,

  -- second batter
  sb.batter        as second_batter,
  sb.total_runs    as second_batter_runs,
  sb.total_balls   as second_batter_balls,

  -- top bowler
  tbo.bowler       as top_bowler,
  tbo.total_wickets as top_bowler_wickets,
  tbo.runs_conceded as top_bowler_runs_conceded,
  tbo.balls_bowled  as top_bowler_balls,

  -- second bowler
  sbo.bowler       as second_bowler,
  sbo.total_wickets as second_bowler_wickets,
  sbo.runs_conceded as second_bowler_runs_conceded,
  sbo.balls_bowled  as second_bowler_balls,

  m.toss_winner,
  m.toss_decision,
  m.match_winner,
  m.margin,
  m.pom

from inning_totals it

join match_meta m
  using(match_id)

left join batters_ranked tb
  on tb.match_id = it.match_id
 and tb.inning_id = it.inning_id
 and tb.rn = 1

left join batters_ranked sb
  on sb.match_id = it.match_id
 and sb.inning_id = it.inning_id
 and sb.rn = 2

left join bowlers_ranked tbo
  on tbo.match_id = it.match_id
 and tbo.inning_id = it.inning_id
 and tbo.rn = 1

left join bowlers_ranked sbo
  on sbo.match_id = it.match_id
 and sbo.inning_id = it.inning_id
 and sbo.rn = 2

order by m.match_id, it.inning_id
