SELECT season, city_cleaned city, 
COALESCE(match_number,playoff_stage) match_num,
 venue_cleaned venue, 
 match_date,
 match_time,
 team1,
 team2
FROM {{ ref('int_match_info') }}
order by season,match_date,match_num