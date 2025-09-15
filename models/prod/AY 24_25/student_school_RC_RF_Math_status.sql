with student_school_baseline as (
    select distinct
        d.student_id,
        d.student_name_base,
        d.city_base,
        d.PM_name_base,
        d.school_name_base,
        d.fellow_name_base,
        d.grade_taught_base,
        f.math_status_base,
        f.RC_status_base,
        f.RF_status_base,
        d.baseline_attendence
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
),

student_school_midline as (
    select distinct
        d.student_id,
        d.student_name_mid,
        d.city_mid,
        d.PM_name_mid,
        d.school_name_mid,
        d.fellow_name_mid,
        d.grade_taught_mid,
        f.math_status_mid,
        f.RC_status_mid,
        f.RF_status_mid,
        d.midline_attendence
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.midline_attendence = True
),

student_school_endline as (
    select distinct
        d.student_id,
        d.student_name_end,
        d.city_end,
        d.PM_name_end,
        d.school_name_end,
        d.fellow_name_end,
        d.grade_taught_end,
        f.math_status_end,
        f.RC_status_end,
        f.RF_status_end,
        d.endline_attendence
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.endline_attendence = True
),

all_combinations as (
    select distinct
        student_id
    from student_school_baseline
    
    union
    
    select distinct
        student_id
    from student_school_midline
    
    union
    
    select distinct
        student_id
    from student_school_endline
)

select 
    ac.student_id,
    
    -- Baseline Details
    b.student_name_base,
    b.city_base,
    b.PM_name_base,
    b.school_name_base,
    b.fellow_name_base,
    b.grade_taught_base,
    b.math_status_base,
    b.RC_status_base,
    b.RF_status_base,
    b.baseline_attendence,
    
    -- Midline Details
    m.student_name_mid,
    m.city_mid,
    m.PM_name_mid,
    m.school_name_mid,
    m.fellow_name_mid,
    m.grade_taught_mid,
    m.math_status_mid,
    m.RC_status_mid,
    m.RF_status_mid,
    m.midline_attendence,
    
    -- Endline Details
    e.student_name_end,
    e.city_end,
    e.PM_name_end,
    e.school_name_end,
    e.fellow_name_end,
    e.grade_taught_end,
    e.math_status_end,
    e.RC_status_end,
    e.RF_status_end,
    e.endline_attendence
from all_combinations ac
left join student_school_baseline b
    on ac.student_id = b.student_id
left join student_school_midline m
    on ac.student_id = m.student_id
left join student_school_endline e
    on ac.student_id = e.student_id
