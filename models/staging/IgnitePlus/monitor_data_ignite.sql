{{ config(
  materialized='table',
  tags=["igniteplus", "staging"]
) }}

with cleaned as (
    select
        coalesce(btrim("S_NO"), '') as s_no,
        coalesce(btrim("Project_ID"), '') as project_id,
        case
            when btrim("Intervention_Year") ~ '^\d{2}-\d{2}$'
                then
                    '20' || substring(btrim("Intervention_Year") from '^(\d{2})')
                    || '-' || '20' || substring(btrim("Intervention_Year") from '-(\d{2})$')
            when btrim("Intervention_Year") ~ '^\d{4}-\d{4}$'
                then btrim("Intervention_Year")
        end as intervention_year,

        case
            when btrim("Monitored_Year") ~ '^\d{2}-\d{2}$'
                then
                    '20' || substring(btrim("Monitored_Year") from '^(\d{2})')
                    || '-' || '20' || substring(btrim("Monitored_Year") from '-(\d{2})$')
            when btrim("Monitored_Year") ~ '^\d{4}-\d{4}$'
                then btrim("Monitored_Year")
        end as monitored_year,
        coalesce(initcap(btrim("Quarter")), '') as quarter,

        coalesce(initcap(btrim("Name_of_School___Address")), '') as school_address,
        coalesce(initcap(btrim("Name_of_District__State")), '') as district,

        case
            when btrim("Date_of_Visit") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                then to_date(
                    regexp_replace(btrim("Date_of_Visit"), '\s*-\s*', '-', 'g'),
                    'DD-Mon-YYYY'
                )
        end as date_of_visit,

        coalesce(initcap(btrim("Visited_By")), '') as visited_by,
        
        coalesce(btrim("Folder_URL_Link"), '') as folder_link
    from {{ source('iginteplus_25_26', 'Monitored_Projects') }}
)

select 
    s_no,
    project_id,
    monitored_year,
    intervention_year,
    quarter,
    school_address,
    district,
    date_of_visit,
    visited_by,
    folder_link
from cleaned
where school_address is not null
