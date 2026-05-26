{{ config(
    materialized='table',
    tags=["civic_clubs"]
) }}

with base as (
    select
        name,
        attendance,
        city,
        state,
        to_timestamp(event_date, 'DD-Mon-YYYY')::date as event_date_parsed
    from {{ ref('cc_volunteers_eventregn_stg') }}
    where name is not null and btrim(name) != ''
),

with_period as (
    select
        name,
        attendance,
        city,
        state,
        to_char(event_date_parsed, 'Month') as month,
        case
            when extract(month from event_date_parsed) >= 4
                then lpad((extract(year from event_date_parsed)::int % 100)::text, 2, '0')
                    || '-'
                    || lpad(((extract(year from event_date_parsed)::int + 1) % 100)::text, 2, '0')
            else
                lpad(((extract(year from event_date_parsed)::int - 1) % 100)::text, 2, '0')
                    || '-'
                    || lpad((extract(year from event_date_parsed)::int % 100)::text, 2, '0')
        end as academic_year
    from base
    where attendance = 'Present'
)

select
    month,
    academic_year,
    city,
    state,
    count(distinct name) as active_volunteer_count
from with_period
group by month, academic_year, city, state
