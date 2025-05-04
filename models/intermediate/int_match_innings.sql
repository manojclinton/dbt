with cte as(

select * 
from {{ ref('stg_match_innings') }})

select cte.*, season, match_id FROM cte
left join {{ ref('int_match_info') }} m_inf on SPLIT(cte.file_name, '.')[OFFSET(0)]=m_inf.match_id
