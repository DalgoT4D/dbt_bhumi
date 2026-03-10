with planned_sessions as (
    select
        donor,
        project_name,
        trainer_name,
        academic_year,
        school_name,
        class_number,
        class_division,
        sum(session_planned) as sessions_planned
    from {{ ref('stem_all_session_plannings_stg') }}
    group by
        donor,
        project_name,
        trainer_name,
        academic_year,
        school_name,
        class_number,
        class_division
),

conducted_sessions as (
    select
        a.donor,
        a.project_name,
        a.trainer_name,
        a.academic_year,
        a.school_name,
        a.class_number,
        a.class_division,
        count(a.id) as sessions_conducted
    from {{ ref('stem_all_session_attendances_stg') }} as a
    group by
        a.donor,
        a.project_name,
        a.trainer_name,
        a.academic_year,
        a.school_name,
        a.class_number,
        a.class_division
)

select
    coalesce(p.donor, a.donor) as donor,
    coalesce(p.project_name, a.project_name) as project_name,
    coalesce(p.trainer_name, a.trainer_name) as trainer_name,
    coalesce(p.academic_year, a.academic_year) as academic_year,
    coalesce(p.school_name, a.school_name) as school_name,
    coalesce(p.class_number, a.class_number) as class_number,
    coalesce(p.class_division, a.class_division) as class_division,
    coalesce(p.sessions_planned, 0) as sessions_planned,
    coalesce(a.sessions_conducted, 0) as sessions_conducted
from planned_sessions as p
full outer join conducted_sessions as a
    on
        p.donor = a.donor
        and p.project_name = a.project_name
        and p.trainer_name = a.trainer_name
        and p.academic_year = a.academic_year
        and p.school_name = a.school_name
        and p.class_number = a.class_number
        and p.class_division = a.class_division
order by
    donor,
    project_name,
    trainer_name,
    academic_year,
    school_name,
    class_number,
    class_division
