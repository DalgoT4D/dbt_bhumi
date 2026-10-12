{{ config(
  materialized='table',
  tags=["igniteplus", "int"]
) }}

select
    mast.*,
    md.intervention_year,
    md.monitored_year,
    md.date_of_visit,
    md.visited_by,
    md.folder_link
from {{ ref('master_data_ign_25_26') }} as mast
left join {{ ref('monitor_data_ignite') }} as md
    on mast.project_id = md.project_id
