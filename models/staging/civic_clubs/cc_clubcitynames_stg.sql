{{ config(
    materialized='table',
    tags=["civic_clubs"]
) }}

select
    coalesce(btrim("ID"::text), '') as id,
    coalesce(initcap(btrim("City"::text)), '') as city,
    coalesce(initcap(btrim("Status"::text)), '') as status,
    coalesce(initcap(btrim("Club_Name"::text)), '') as club_name,
    case
        when btrim("Added_Time"::text) ~ '^\d{4}-\d{2}-\d{2}' then btrim("Added_Time"::text)::timestamp
    end as added_time
from {{ source('zc_bvms_data', 'Club_City_and_Names_Report') }}
