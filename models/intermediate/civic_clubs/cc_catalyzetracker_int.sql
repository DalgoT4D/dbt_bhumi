{{ config(
    materialized='table',
    tags=["civic_clubs"]
) }}

with joined as (
    select
        c.id as club_id,
        c.club_name,
        c.city as club_city,
        t.event_id,
        t.event_name,
        to_timestamp(t.event_date, 'DD-Mon-YYYY')::date as event_date,
        t.event_category,
        t.cause as event_cause,
        t.number_of_volunteers,
        t.total_volunteer_hours
    from {{ ref('cc_clubcitynames_stg') }} as c
    left join {{ ref('cc_catalyzetracker_stg') }} as t
        on c.id = t.club_id
)

select
    club_id,
    club_name,
    club_city,
    event_id,
    event_name,
    event_date,
    to_char(event_date, 'Month') as month,
    case
        when extract(month from event_date) >= 4
            then lpad((extract(year from event_date)::int % 100)::text, 2, '0')
                || '-'
                || lpad(((extract(year from event_date)::int + 1) % 100)::text, 2, '0')
        else
            lpad(((extract(year from event_date)::int - 1) % 100)::text, 2, '0')
                || '-'
                || lpad((extract(year from event_date)::int % 100)::text, 2, '0')
    end as academic_year,
    event_category,
    event_cause,
    number_of_volunteers,
    total_volunteer_hours
from joined
