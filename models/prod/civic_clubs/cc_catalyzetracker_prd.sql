{{ config(
    materialized='table',
    tags=["civic_clubs", "prod"]
) }}

with aggregated as (
    select
        club_id,
        club_name,
        club_city,
        event_category,
        event_cause,
        academic_year,
        month,
        sum(number_of_volunteers) as total_volunteers,
        sum(total_volunteer_hours) as total_volunteer_hours,
        count(event_id) as num_events
    from {{ ref('cc_catalyzetracker_int') }}
    group by club_id, club_name, club_city, event_category, event_cause, academic_year, month
),

club_totals as (
    select
        club_id,
        sum(num_events) as total_events
    from aggregated
    group by club_id
)

select
    a.club_id,
    a.club_name,
    a.club_city,
    a.event_category,
    a.event_cause,
    a.academic_year,
    a.month,
    a.total_volunteers,
    a.total_volunteer_hours,
    a.num_events,
    case
        when ct.total_events > 0 then 'Active'
        else 'Inactive'
    end as club_status
from aggregated as a
left join club_totals as ct on a.club_id = ct.club_id
