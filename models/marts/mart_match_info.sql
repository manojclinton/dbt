{{ config(materialized='table') }}

WITH 

base AS (
  SELECT
    *,
    -- your existing cleaning logic
    COALESCE(
      city,
      SPLIT(venue, ' ')[OFFSET(0)]
    ) AS city_cleaned,

    COALESCE(
      SPLIT(venue, ',')[OFFSET(0)],
      venue
    ) AS venue_cleaned

  FROM {{ ref('stg_match_info') }}
),

venue_map AS (
  -- build the distinct lookup of venue_cleaned → venue_id
  SELECT
    venue_cleaned,
    DENSE_RANK() OVER (ORDER BY venue_cleaned) AS venue_id
  FROM (
    SELECT DISTINCT venue_cleaned
    FROM base
  )
),

enriched AS (
  -- join your base rows back to that lookup
  SELECT
    b.*,
    v.venue_id
  FROM base AS b
  LEFT JOIN venue_map AS v
    USING (venue_cleaned)
)

SELECT * FROM enriched

