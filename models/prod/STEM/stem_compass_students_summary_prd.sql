select
    donor,
    state,
    district,
    trainer_name,
    reporting_manager_name,
    status,
    academic_year,
    count(distinct id) as student_count
from {{ ref('stem_compass_all_students_stg') }}
group by
    donor,
    state,
    district,
    trainer_name,
    reporting_manager_name,
    status,
    academic_year
