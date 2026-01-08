with math_analysis_baseline as (
    select
        d.city_base as city,
        d.student_grade_base as grade,
        f.math_learning_level_status_baseline_base as math_status,
        count(distinct f.student_id) as student_count_base
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, d.student_grade_base, f.math_learning_level_status_baseline_base
),

-- math_analysis_midline as (
--     select
--         d.city_mid as city,
--         d.grade_taught_mid as grade,
--         f.math_status_mid as math_status,
--         count(distinct f.student_id) as student_count_mid
--     from 
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
--         on f.student_id = d.student_id
--     where d.midline_attendence = True
--     group by d.city_mid, d.grade_taught_mid, f.math_status_mid
-- ),

-- math_analysis_endline as (
--     select
--         d.city_end as city,
--         d.grade_taught_end as grade,
--         f.math_status_end as math_status,
--         count(distinct f.student_id) as student_count_end
--     from 
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
--         on f.student_id = d.student_id
--     where d.endline_attendence = True
--     group by d.city_end, d.grade_taught_end, f.math_status_end
-- ),

all_combinations as (
    select distinct
        city,
        grade,
        math_status
    from math_analysis_baseline
    
    -- union
    
    -- select distinct
    --     city,
    --     grade,
    --     math_status
    -- from math_analysis_midline
    
    -- union
    
    -- select distinct
    --     city,
    --     grade,
    --     math_status
    -- from math_analysis_endline
)

select 
    ac.city,
    ac.grade,
    ac.math_status,
    b.student_count_base
    -- m.student_count_mid,
    -- e.student_count_end
from all_combinations as ac
left join math_analysis_baseline as b
    on
        ac.city = b.city 
        and ac.math_status = b.math_status 
        and ac.grade = b.grade
-- left join math_analysis_midline m
--     on ac.city = m.city 
--     and ac.math_status = m.math_status 
--     and ac.grade = m.grade
-- left join math_analysis_endline e
--     on ac.city = e.city 
--     and ac.math_status = e.math_status 
--     and ac.grade = e.grade
-- order by ac.city, ac.grade, ac.math_status
