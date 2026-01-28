with student_school_baseline as (
    select distinct
        d.student_id,
        d.student_name_base,
        d.city_base,
        d.pm_name_base,
        d.school_name_base,
        d.fellow_name_base,
        d.student_grade_base,
        f.rc_learning_level_status_baseline_base,
        f.math_learning_level_status_baseline_base,
        f.rf_level_baseline_base,
        d.baseline_attendence
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
),

student_school_midline as (
    select distinct
        d.student_id,
        d.student_name_mid,
        d.city_mid,
        d.pm_name_mid,
        d.school_name_mid,
        d.fellow_name_mid,
        d.student_grade_mid,
        f.rc_learning_level_status_midline_mid,
        f.math_learning_level_status_midline_mid,
        f.rf_level_midline_mid,
        d.midline_attendence
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as d
        on f.student_id = d.student_id
    where d.midline_attendence = True
),

-- student_school_endline as (
--     select distinct
--         d.student_id,
--         d.student_name_end,
--         d.city_end,
--         d.PM_name_end,
--         d.school_name_end,
--         d.fellow_name_end,
--         d.grade_taught_end,
--         f.rc_learning_level_status_baseline_base,
--         f.math_learning_level_status_baseline_base,
--         d.endline_attendence
--     from 
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
--         on f.student_id = d.student_id
--     where d.endline_attendence = True
-- ),

all_combinations as (
    select distinct student_id
    from student_school_baseline
    
    union
    
    select distinct student_id
    from student_school_midline
    
    -- union
    
    -- select distinct
    --     student_id
    -- from student_school_endline
)

select 
    ac.student_id,
    
    -- Baseline Details
    b.student_name_base,
    b.city_base,
    b.pm_name_base,
    b.school_name_base,
    b.fellow_name_base,
    b.student_grade_base,
    b.rc_learning_level_status_baseline_base,
    b.math_learning_level_status_baseline_base,
    b.rf_level_baseline_base,
    b.baseline_attendence,
    
    -- Midline Details
    m.student_name_mid,
    m.city_mid,
    m.pm_name_mid,
    m.school_name_mid,
    m.fellow_name_mid,
    m.student_grade_mid,
    m.rc_learning_level_status_midline_mid,
    m.math_learning_level_status_midline_mid,
    m.rf_level_midline_mid,
    m.midline_attendence
    
    -- -- Endline Details
    -- e.student_name_end,
    -- e.city_end,
    -- e.PM_name_end,
    -- e.school_name_end,
    -- e.fellow_name_end,
    -- e.grade_taught_end,
    -- e.math_status_end,
    -- e.RC_status_end,
    -- e.RF_status_end,
    -- e.endline_attendence
from all_combinations as ac
left join student_school_baseline as b
    on ac.student_id = b.student_id
left join student_school_midline as m
    on ac.student_id = m.student_id
-- left join student_school_endline e
--     on ac.student_id = e.student_id
