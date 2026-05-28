{{ config(
    materialized='table',
    tags=["civic_clubs", "prod"]
) }}

with stg as (
    select * from {{ ref('cc_nps_stg') }}
),

with_academic_year as (
    select
        quarter,
        case
            when extract(month from submission_date) >= 4
                then
                    lpad((extract(year from submission_date)::int % 100)::text, 2, '0')
                    || '-'
                    || lpad(((extract(year from submission_date)::int + 1) % 100)::text, 2, '0')
            else
                lpad(((extract(year from submission_date)::int - 1) % 100)::text, 2, '0')
                || '-'
                || lpad((extract(year from submission_date)::int % 100)::text, 2, '0')
        end as academic_year,
        program,
        sub_programs,
        utm_source as source,
        rating,
        email
    from stg
)

select
    quarter,
    academic_year,
    program,
    sub_programs,
    source,
    count(distinct email) as volunteer_count,
    avg(rating) as avg_rating
from with_academic_year
group by quarter, academic_year, program, sub_programs, source
