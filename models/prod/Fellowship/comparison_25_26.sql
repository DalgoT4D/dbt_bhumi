with all_student_id as (
    select distinct
        student_id_base as student_id,
        school_name_base as school_name,
        classroom_id_base as classroom_id,
        city_base as city,
        student_name_base as student_name,
        pm_name_base as pm_name,
        fellow_name_base as fellow_name,
        student_grade_base as student_grade,
        rc_learning_level_status_baseline_base as rc_status,
        rf_baseline_growth_base as rf_status,
        math_learning_level_status_baseline_base as math_status
    from {{ ref('baseline_25_26_stg') }}
    where student_id_base is not null
    
    -- Uncomment the following unions if needed
    union

    select distinct
        student_id_mid as student_id,
        school_name_mid as school_name,
        classroom_id_mid as classroom_id,
        city_mid as city,
        student_name_mid as student_name,
        pm_name_mid as pm_name,
        fellow_name_mid as fellow_name,
        student_grade_mid as student_grade,
        rc_learning_level_status_midline_mid as rc_status,
        rf_midline_growth_mid as rf_status,
        math_learning_level_status_midline_mid as math_status
    from {{ ref('midline_25_26_stg') }}
    where student_id_mid is not null

    -- union

    -- select distinct 
    --     student_id_end as student_id
    -- from {{ ref('endline_2425_stg') }}
    -- where student_id_end is not null
)

select 
    s.student_id,
    s.school_name,
    s.classroom_id,
    s.city,
    s.student_name,
    s.pm_name,
    s.fellow_name,
    s.student_grade,
    s.rc_status,
    s.rf_status,
    s.math_status,

    -- Baseline columns
    b.student_id_base is not null as baseline_attendance,
    b.rc_learning_level_status_baseline_base,
    b.rf_baseline_growth_base,
    b.math_learning_level_status_baseline_base,
    b.cohort_base,

    -- Midline columns
    m.student_id_mid is not null as midline_attendance,
    m.rc_learning_level_status_midline_mid,
    m.rf_midline_growth_mid,
    m.math_learning_level_status_midline_mid,
    m.cohort_mid
    -- Endline columns

from all_student_id as s
left join {{ ref('baseline_25_26_stg') }} as b 
    on s.student_id = b.student_id_base
left join {{ ref('midline_25_26_stg') }} as m 
    on s.student_id = m.student_id_mid
-- left join {{ ref('endline_2425_stg') }} e 
--     on s.student_id = e.student_id_end
