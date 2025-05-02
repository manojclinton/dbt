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
,
new_base AS(
    SELECT
    *,
    COALESCE(
        city,
        SPLIT(venue, ' ')[OFFSET(0)]
        ) AS city_cleaned,

        COALESCE(
        SPLIT(venue, ',')[OFFSET(0)],
        venue
        ) AS venue_cleaned,

    CASE
        WHEN matches_per_day = 1 THEN '19:30'
        WHEN matches_per_day = 2
        AND match_order      = 1 THEN '15:30'
        WHEN matches_per_day = 2
        AND match_order      = 2 THEN '19:30'
        ELSE '19:30'
    END AS match_time

FROM match_sorted

),

venue_map AS (
  -- build the distinct lookup of venue_cleaned → venue_id
  SELECT
    venue_cleaned,
    DENSE_RANK() OVER (ORDER BY venue_cleaned) AS venue_id
  FROM (
    SELECT DISTINCT venue_cleaned
    FROM new_base
  )
),

enriched AS (
  -- join THE NEW base rows back to that lookup
  SELECT
    b.*,
    v.venue_id
  FROM new_base AS b
  LEFT JOIN venue_map AS v
    USING (venue_cleaned)
)

SELECT * FROM enriched