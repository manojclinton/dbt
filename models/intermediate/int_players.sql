with cte as(
    SELECT
    player_name,
    team_name
    FROM {{ref("stg_cricket_match")}}  AS t

    -- 1) Pull out the team names (the keys of the players object)
    ,UNNEST(
    JSON_KEYS(t.raw_file.info.players)
    ) AS team_name

    -- 2) For each team, sub-script into the JSON to get its array of player names,
    --    extract that as an ARRAY<STRING>, and flatten it
    , UNNEST(
    JSON_VALUE_ARRAY(
        t.raw_file.info.players[team_name]
    )
    ) AS player_name
)

select team_name,player_name,pi.*

from cte
left join {{ ref('stg_players_info') }} as pi on cte.player_name=pi.name