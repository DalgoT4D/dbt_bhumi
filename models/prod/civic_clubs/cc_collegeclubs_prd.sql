{{ config(
    materialized='table',
    tags=["civic_clubs", "prod"]
) }}

select
    city,
    state,
    status,
    academic_year,
    count(college_club_name) as club_count
from {{ ref('cc_collegeclubs_stg') }}
where college_club_name is not null and btrim(college_club_name) != ''
group by city, state, status, academic_year
