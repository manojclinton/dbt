with stadium_lookup AS (
  SELECT
    venue_id,
    MIN(latitude)  AS latitude,
    MIN(longitude) AS longitude
  FROM {{ ref('int_stadiums_info') }}
  GROUP BY venue_id
),

matches_cte as(
    SELECT season, match_id, city_cleaned city, 
    COALESCE(match_number,playoff_stage) match_num,
    venue_cleaned venue, 
    match_date,
    match_time,
    team1,
    team2,
    venue_id
    FROM {{ ref('int_match_info') }}

)


SELECT
  u.*,
  c.latitude,
  c.longitude
FROM matches_cte AS u
LEFT JOIN stadium_lookup AS c
  USING (venue_id)
ORDER BY season,match_date,match_num
