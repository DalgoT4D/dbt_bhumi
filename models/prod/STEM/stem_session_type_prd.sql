select
    donor,
    project_name,
    trainer_name,
    academic_year,
    school_name,
    class_number,
    class_division,
    session_type,
    count(id) as session_count
from {{ ref('stem_all_session_attendances_stg') }}
group by
    donor,
    project_name,
    trainer_name,
    academic_year,
    school_name,
    class_number,
    class_division,
    session_type
order by
    academic_year,
    donor,
    project_name,
    trainer_name,
    school_name,
    class_number,
    class_division,
    session_type
