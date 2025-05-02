with match_sorted as(
    SELECT *,
     COUNT(*) OVER (PARTITION BY match_date) AS matches_per_day,
     CASE
        WHEN match_number IS NULL THEN 'playoffs'
        ELSE 'league match'
     END AS flag_playoffs,
     row_number() over(PARTITION BY match_date ORDER BY match_number) as match_order
      FROM {{ ref('stg_match_info') }}
)

SELECT
  *,
  CASE
    WHEN matches_per_day = 1 THEN '19:30'
    WHEN matches_per_day = 2
      AND match_order      = 1 THEN '15:30'
    WHEN matches_per_day = 2
      AND match_order      = 2 THEN '19:30'
    ELSE '19:30'
  END AS match_time
FROM match_sorted
order by season,match_date,match_number