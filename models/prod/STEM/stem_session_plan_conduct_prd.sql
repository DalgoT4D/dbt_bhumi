with planned_raw as (
    select
        donor,
        project_name,
        trainer_name,
        academic_year,
        session_month,
        case
            when session_month_number in (4, 5, 6) then 'Q1'
            when session_month_number in (7, 8, 9) then 'Q2'
            when session_month_number in (10, 11, 12) then 'Q3'
            when session_month_number in (1, 2, 3) then 'Q4'
        end as academic_quarter,
        school_name,
        class_number,
        class_division,
        session_planned
    from {{ ref('stem_all_session_plannings_stg') }}
),

planned_sessions as (
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
        sum(session_planned) as sessions_planned
    from planned_raw
    group by
        donor,
        project_name,
        trainer_name,
        academic_year,
        session_month,
        academic_quarter,
        school_name,
        class_number,
        class_division
),

conducted_raw as (
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
        id
    from {{ ref('stem_all_session_attendances_stg') }}
),

conducted_sessions as (
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
        count(id) as sessions_conducted
    from conducted_raw
    group by
        donor,
        project_name,
        trainer_name,
        academic_year,
        session_month,
        academic_quarter,
        school_name,
        class_number,
        class_division
)

select
    coalesce(p.donor, a.donor) as donor,
    coalesce(p.project_name, a.project_name) as project_name,
    coalesce(p.trainer_name, a.trainer_name) as trainer_name,
    coalesce(p.academic_year, a.academic_year) as academic_year,
    coalesce(p.session_month, a.session_month) as session_month,
    coalesce(p.academic_quarter, a.academic_quarter) as academic_quarter,
    coalesce(p.school_name, a.school_name) as school_name,
    coalesce(p.class_number, a.class_number) as class_number,
    coalesce(p.class_division, a.class_division) as class_division,
    coalesce(p.sessions_planned, 0) as sessions_planned,
    coalesce(a.sessions_conducted, 0) as sessions_conducted
from planned_sessions as p
full outer join conducted_sessions as a
    on
        p.academic_year = a.academic_year
        and p.academic_quarter = a.academic_quarter
        and p.session_month = a.session_month
        and p.school_name = a.school_name
        and p.class_number = a.class_number
        and p.class_division = a.class_division
order by
    donor,
    project_name,
    trainer_name,
    academic_year,
    session_month,
    academic_quarter,
    school_name,
    class_number,
    class_division
