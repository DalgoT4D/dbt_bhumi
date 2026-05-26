{{ config(
    materialized='table',
    tags=["civic_clubs", "prod"]
) }}

with base as (
    select
        name,
        to_char(added_date, 'Month') as month,
        case
            when extract(month from added_date) >= 4
                then
                    lpad((extract(year from added_date)::int % 100)::text, 2, '0')
                    || '-'
                    || lpad(((extract(year from added_date)::int + 1) % 100)::text, 2, '0')
            else
                lpad(((extract(year from added_date)::int - 1) % 100)::text, 2, '0')
                || '-'
                || lpad((extract(year from added_date)::int % 100)::text, 2, '0')
        end as academic_year,
        attendance,
        city,
        state,
        event_name
    from {{ ref('cc_volunteers_inductionregn_stg') }}
)

select
    month,
    academic_year,
    attendance,
    city,
    state,
    event_name,
    count(name) as volunteer_count
from base
group by month, academic_year, attendance, city, state, event_name
