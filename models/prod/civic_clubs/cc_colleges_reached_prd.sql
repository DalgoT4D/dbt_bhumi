{{ config(
    materialized='table',
    tags=["civic_clubs"]
) }}

with combined as (
    select city, state, college_club_name as institution_name
    from {{ ref('cc_collegeclubs_stg') }}

    union

    select city, state, institute as institution_name
    from {{ ref('cc_allinstitutemasters_stg') }}
)

select
    city,
    state,
    count(distinct institution_name) as total_institutions
from combined
where institution_name is not null and btrim(institution_name) != ''
group by city, state
