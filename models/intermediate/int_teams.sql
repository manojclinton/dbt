with cte as(

SELECT
  -- Extract each array element (a JSON string) and pull out its scalar value
  JSON_EXTRACT_SCALAR(team_json, '$')        AS team_name,
  -- Extract the team_type field from the JSON object
  JSON_EXTRACT_SCALAR(raw_file, '$.info.team_type') AS team_type
FROM {{ref("stg_cricket_match")}},
  -- Turn the JSON array at $.info.teams into one row per element
  UNNEST(
    JSON_EXTRACT_ARRAY(raw_file, '$.info.teams')
  ) AS team_json
GROUP BY
  team_name,
  team_type
)

select cte.*, team_id,image_url
    from cte
    left join {{ ref('stg_ipl_teams') }} t on t.team=cte.team_name