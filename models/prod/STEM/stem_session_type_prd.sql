{{ config(
  materialized='table',
  tags=["stem", "prod"]
) }}

with sessions_raw as (
    select
        donor,
        project_name,
        trainer_name,
        trim(to_char(session_date, 'Month')) as session_month,
        case
            when extract(month from session_date)::integer in (4, 5, 6) then 'Q1'
            when extract(month from session_date)::integer in (7, 8, 9) then 'Q2'
            when extract(month from session_date)::integer in (10, 11, 12) then 'Q3'
            when extract(month from session_date)::integer in (1, 2, 3) then 'Q4'
        end as academic_quarter,
        case
            when extract(month from session_date) >= 4
                then extract(year from session_date)::integer::text || '-' || (extract(year from session_date)::integer + 1)::text
            else (extract(year from session_date)::integer - 1)::text || '-' || extract(year from session_date)::integer::text
        end as academic_year,
        school_name,
        class_number,
        class_division,
        session_type,
        id
    from {{ ref('stem_all_session_attendances_stg') }}
)

select
    donor,
    project_name,
    trainer_name,
    academic_year,
    session_month,
    academic_quarter,
    school_name,
    class_number,
    class_division,
    session_type,
    count(id) as session_count
from sessions_raw
group by
    donor,
    project_name,
    trainer_name,
    academic_year,
    session_month,
    academic_quarter,
    school_name,
    class_number,
    class_division,
    session_type
order by
    academic_year,
    academic_quarter,
    donor,
    project_name,
    trainer_name,
    school_name,
    class_number,
    class_division,
    session_type
