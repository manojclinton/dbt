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

),

final as(
SELECT
  u.*,
  c.latitude,
  c.longitude,
FROM matches_cte AS u
LEFT JOIN stadium_lookup AS c
  USING (venue_id))

select f.* ,temp_c,
    humidity_percent,
    pressure_hpa,
    cloudcover_percent,
    rain_mm,
    wind_m_s 
from final f
join {{ ref('int_weather_info') }} w
ON CAST(w.match_id AS STRING) = CAST(f.match_id AS STRING)

ORDER BY season,match_date,match_num