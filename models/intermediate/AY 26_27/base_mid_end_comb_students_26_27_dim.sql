{{ config(
  materialized='table',
  tags=["ay_26_27", "int"]
) }}

with all_student_id as (
    select distinct  student_id_base as student_id
    from {{ ref('baseline_26_27_stg') }}
    where student_id_base is not null
    
    -- Uncomment the following unions if needed
    -- union

    -- union

)

select 
    s.student_id,
    -- Baseline columns
    not coalesce(b.student_id_base is null, false) as baseline_attendence,
    b.city_base,
    b.student_name_base,
    b.classroom_id_base,
    b.pm_name_base,
    b.school_name_base,
    b.fellow_name_base,
    b.cohort_base,
    b.student_grade_base,
    b.donor_base
    -- Midline columns
    -- not coalesce(m.student_id_mid is null, false) as midline_attendence,
    -- m.city_mid,
    -- m.student_name_mid,
    -- m.classroom_id_mid,
    -- m.pm_name_mid,
    -- m.school_name_mid,
    -- m.fellow_name_mid,
    -- m.cohort_mid,
    -- m.student_grade_mid,
    -- m.donor_mid,
    -- Endline columns
    -- not coalesce(e.student_id_end is null, false) as endline_attendence,
    -- e.city_end,
    -- e.student_name_end,
    -- e.classroom_id_end,
    -- e.pm_name_end,
    -- e.school_name_end,
    -- e.fellow_name_end,
    -- e.cohort_end,
    -- e.student_grade_end,
    -- e.donor_end

from all_student_id as s
left join {{ ref('baseline_26_27_stg') }} as b 
    on s.student_id = b.student_id_base
