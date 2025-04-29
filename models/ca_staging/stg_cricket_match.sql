with cte as (
SELECT * FROM {{ source('cricket_raw', 'cricket_match_raw') }}
)

select file_name, content as raw_file 
from cte