{{ config(materialized='table') }}

WITH base AS (
  SELECT
    *,
    -- if city is NULL, take first token of venue
    COALESCE(
      city,
      SPLIT(venue, ' ')[OFFSET(0)]
    ) AS city_cleaned,

    -- split off after comma, OR fall back to full venue if no comma
    COALESCE(
      SPLIT(venue, ',')[OFFSET(0)],
      venue
    ) AS venue_cleaned

  FROM {{ ref('stg_match_info') }}
)
,
base_mit_id as (
SELECT
  *,
  -- 1,2,3… IDs over every distinct cleaned-or-original key
  DENSE_RANK() OVER (
    ORDER BY COALESCE(venue_cleaned, venue)
  ) AS venue_id

FROM base)
,

unique_stadiums as (
select distinct venue_id,city_cleaned,venue_cleaned from base_mit_id
)

select u.*,c.latitude,c.longitude from
unique_stadiums u
join {{ ref('stg_stadium_coordinates') }} c on u.venue_id=c.venue_id
order by venue_id


