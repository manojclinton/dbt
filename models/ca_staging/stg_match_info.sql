SELECT
  JSON_VALUE(raw_file, '$.info.match_type')                            AS match_type,
  JSON_VALUE(raw_file, '$.info.gender')                                AS gender,
  JSON_VALUE(raw_file, '$.info.season')                                AS season,
  JSON_VALUE(raw_file, '$.info.city')                                  AS city,
  JSON_VALUE(raw_file, '$.info.venue')                                 AS venue,
  JSON_VALUE(raw_file, '$.info.event.match_number')                   AS match_number,
  CAST(JSON_VALUE(raw_file, '$.info.dates[0]') AS DATE)                AS match_date,
  JSON_VALUE(raw_file, '$.info.teams[0]')                              AS team1,
  JSON_VALUE(raw_file, '$.info.teams[1]')                              AS team2,
  JSON_VALUE(raw_file, '$.info.toss.winner')                           AS toss_winner,
  JSON_VALUE(raw_file, '$.info.toss.decision')                         AS toss_decision,
  JSON_VALUE(raw_file, '$.info.outcome.winner')                        AS match_winner,
  CASE
    WHEN JSON_VALUE(raw_file, '$.info.outcome.by.wickets') IS NOT NULL
      THEN CONCAT(JSON_VALUE(raw_file, '$.info.outcome.by.wickets'), ' wickets')
    WHEN JSON_VALUE(raw_file, '$.info.outcome.by.runs')    IS NOT NULL
      THEN CONCAT(JSON_VALUE(raw_file, '$.info.outcome.by.runs'),    ' runs')
    ELSE 'no result'
  END                                                                   AS margin,
  JSON_VALUE(raw_file, '$.info.event.name')                            AS event_name,
  JSON_VALUE(raw_file, '$.info.team_type')                             AS event_type,
  JSON_VALUE(raw_file, '$.info.player_of_match[0]')                    AS pom,
  file_name                                                            AS filename
FROM {{ ref('stg_cricket_match') }}

