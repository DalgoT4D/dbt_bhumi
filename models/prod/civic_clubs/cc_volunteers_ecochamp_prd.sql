{{ config(
    materialized='table',
    tags=["civic_clubs", "prod"]
) }}

select
    city,
    null as state,
    null::text as month,
    '2025-2026' as academic_year,
    school_shelter_name,
    count(distinct volunteer_name) as volunteer_count,
    sum(total_volunteer_hours) as total_volunteer_hours,
    sum(total_sessions_attended) as total_sessions_attended
from {{ ref('cc_volunteers_ecochamp_stg') }}
group by city, school_shelter_name
