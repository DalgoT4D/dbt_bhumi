{{ config(
  materialized='table',
  tags=["fsa_25_26", "int"]
) }}

select distinct
    fy.funding_year,
    fy.school_id,
    s.school_name,
    s.school_state,
    s.city,
    s.is_active,
    s.udise_code,
    s.school_type,
    fy.male_students as male_students_in_school,
    fy.total_students as total_students_in_school,
    fy.female_students as female_students_in_school,
    fy.total_support_staff as total_support_staff_in_school,
    fy.total_teaching_staff as total_teaching_staff_in_school
from {{ ref('school_fy_data') }} as fy
left join {{ ref('school_data') }} as s
    on fy.school_id = s.school_id
