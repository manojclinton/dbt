WITH exploded AS (

  SELECT
    t.file_name,

    -- 1) inning index + 1
    inning_idx + 1                                          AS inning_id,

    -- 2) over number + 1
    CAST(JSON_VALUE(ov, '$.over')  AS INT64) + 1            AS over_number,

    -- 3) batting team
    JSON_VALUE(f0, '$.team')                                AS batting_team,

    -- 4) ball in over
    ball_idx + 1                                            AS ball_in_over,

    -- 5–8) batter, bowler, non_striker
    JSON_VALUE(f1, '$.batter')                              AS batter,
    JSON_VALUE(f1, '$.bowler')                              AS bowler,
    JSON_VALUE(f1, '$.non_striker')                         AS non_striker,

    -- 9) runs to batter
    CAST(JSON_VALUE(f1, '$.runs.batter') AS INT64)          AS runs_batter,

    -- 10) extras key (first one)
    JSON_KEYS(
      JSON_QUERY(f1, '$.extras')
    )[OFFSET(0)]                                            AS extra_type,

    -- 11–12) extras & total runs
    CAST(JSON_VALUE(f1, '$.runs.extras') AS INT64)          AS runs_extras,
    CAST(JSON_VALUE(f1, '$.runs.total')  AS INT64)          AS runs_total,

    -- 13–14) wicket kind & player out
    JSON_VALUE(f1, '$.wickets[0].kind')                     AS dismissal_kind,
    JSON_VALUE(f1, '$.wickets[0].player_out')               AS out_player,

    -- 15–16) powerplay start/end (CEIL after FLOAT64 cast)
    COALESCE(
      CEIL(CAST(JSON_VALUE(f0, '$.powerplays[0].from') AS FLOAT64)),
      dp.start_over
    )                                                       AS powerplays_start_over,

    COALESCE(
      CEIL(CAST(JSON_VALUE(f0, '$.powerplays[0].to')   AS FLOAT64)),
      dp.end_over
    )                                                       AS powerplays_end_over

  FROM {{ ref('stg_cricket_match') }} AS t

  LEFT JOIN {{ ref('default_powerplay') }} AS dp
    ON JSON_VALUE(t.raw_file, '$.info.match_type') = dp.match_type

  -- 1st UNNEST: innings
  CROSS JOIN UNNEST(
    JSON_EXTRACT_ARRAY(t.raw_file, '$.innings')
  ) AS f0
  WITH OFFSET AS inning_idx

  -- 2nd UNNEST: overs
  CROSS JOIN UNNEST(
    JSON_EXTRACT_ARRAY(f0, '$.overs')
  ) AS ov
  WITH OFFSET AS over_idx

  -- 3rd UNNEST: deliveries
  CROSS JOIN UNNEST(
    JSON_EXTRACT_ARRAY(ov, '$.deliveries')
  ) AS f1
  WITH OFFSET AS ball_idx

),

final AS (

  SELECT
    *,
    -- now we *can* refer to those aliases
    CASE
      WHEN over_number 
           BETWEEN powerplays_start_over AND powerplays_end_over
      THEN 1 ELSE 0
    END   AS is_powerplay
  FROM exploded

)

SELECT * FROM final
