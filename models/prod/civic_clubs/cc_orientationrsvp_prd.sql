{{ config(
    materialized='table',
    tags=["civic_clubs"]
) }}

with base as (
    select
        first_name,
        to_char(added_date, 'Month') as month,
        case
            when extract(month from added_date) >= 4
                then lpad((extract(year from added_date)::int % 100)::text, 2, '0')
                    || '-'
                    || lpad(((extract(year from added_date)::int + 1) % 100)::text, 2, '0')
            else
                lpad(((extract(year from added_date)::int - 1) % 100)::text, 2, '0')
                    || '-'
                    || lpad((extract(year from added_date)::int % 100)::text, 2, '0')
        end as academic_year,
        registration_city
    from {{ ref('cc_orientationrsvp_stg') }}
)

select
    month,
    academic_year,
    registration_city,
    count(first_name) as rsvp_count
from base
group by month, academic_year, registration_city
