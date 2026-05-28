{{ config(
    materialized='table',
    tags=["civic_clubs", "prod"]
) }}

select
    academic_year,
    city,
    state,
    null as month,
    sum(leaders) as total_leaders,
    sum(volunteers) as total_volunteers,
    sum(num_of_events) as total_events,
    sum(volunteering_hours) as total_volunteering_hours
from {{ ref('cc_volunteers_collegeclubs_citywise_stg') }}
group by academic_year, city, state
