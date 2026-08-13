{{ config(
  materialized='table',
  tags=["igniteplus", "staging"]
) }}

with cleaned as (
    select
        case
            when btrim("S_NO") ~ '^\d+$'
                then "S_NO"::integer
        end as s_no,
        nullif(btrim("Project_ID"), '') as project_id,

        case
            when btrim("Monitored_Year") ~ '^\d{2}-\d{2}$'
                then
                    '20' || substring(btrim("Monitored_Year") from '^(\d{2})')
                    || '-' || '20' || substring(btrim("Monitored_Year") from '-(\d{2})$')
            when btrim("Monitored_Year") ~ '^\d{4}-\d{4}$'
                then btrim("Monitored_Year")
        end as monitored_year,
        case
            when btrim("Intervention_Year") ~ '^\d{2}-\d{2}$'
                then
                    '20' || substring(btrim("Intervention_Year") from '^(\d{2})')
                    || '-' || '20' || substring(btrim("Intervention_Year") from '-(\d{2})$')
            when btrim("Intervention_Year") ~ '^\d{4}-\d{4}$'
                then btrim("Intervention_Year")
        end as intervention_year,
        
        nullif(initcap(btrim("Visited_By")), '') as visited_by,
        nullif(btrim("Folder_URL_Link"), '') as folder_link,
        case
            when
                btrim("Date_of_Visit") <> ''
                and (
                    btrim("Date_of_Visit") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Date_of_Visit") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{2,4}$'
                    or btrim("Date_of_Visit") ~ '^\d{1,2}\s+[A-Za-z]{3}\s+\d{2,4}$'
                )
                then to_date(
                    case
                        when btrim("Date_of_Visit") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Date_of_Visit")
                        else regexp_replace(btrim("Date_of_Visit"), '\s+', '-', 'g')
                    end,
                    case
                        when btrim("Date_of_Visit") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        when btrim("Date_of_Visit") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$' then 'DD-Mon-YYYY'
                        else 'DD-Mon-YY'
                    end
                )
        end as date_of_visit,
        nullif(initcap(btrim("Name_of_District__State")), '') as district_state,
        nullif(initcap(btrim("Name_of_School___Address")), '') as school_address
        
    from {{ source('iginteplus_25_26', 'Monitored_Projects') }}
)

select 
    s_no,
    project_id,
    monitored_year,
    intervention_year,
    visited_by,
    folder_link,
    date_of_visit,
    district_state,
    school_address
from cleaned
where school_address is not null
