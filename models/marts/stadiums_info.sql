SELECT
  DISTINCT venue, city
FROM {{ ref('stg_match_info') }}