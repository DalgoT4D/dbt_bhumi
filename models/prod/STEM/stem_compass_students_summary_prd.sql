{{ config(
  materialized='table',
  tags=["stem", "prod"]
) }}

select
    donor,
    state,
    district,
    school_name,
    trainer_name,
    reporting_manager_name,
    status,
    gender,
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
    gender,
    academic_year,
    school_name
