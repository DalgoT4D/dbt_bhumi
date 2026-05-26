{{ config(
    materialized='table',
    tags=["civic_clubs"]
) }}

select
    academic_year,
    quarter,
    null as month,
    null as city,
    null as state,
    campaign_name,
    num_event_coordinators,
    num_volunteering_hours,
    num_colleges_engaged,
    num_volunteers_engaged,
    num_events_organized
from {{ ref('cc_campaigns_collegeclubs_stg') }}
