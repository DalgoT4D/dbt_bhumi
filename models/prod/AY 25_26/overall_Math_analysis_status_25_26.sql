with math_analysis_baseline as (
    select
        d.city_base as city,
        d.student_grade_base as grade,
        d.donor_base as donor,
        d.pm_name_base as pm_name,
        d.fellow_name_base as fellow_name,
        f.math_learning_level_status_baseline_base as math_level,
        count(distinct f.student_id) as student_count_base,
        count(distinct case when d.cohort_base = '2024' then f.student_id end) as cohort_2024_count_base,
        count(distinct case when d.cohort_base = '2025' then f.student_id end) as cohort_2025_count_base
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, d.student_grade_base, d.donor_base, d.pm_name_base, d.fellow_name_base, f.math_learning_level_status_baseline_base
),

math_analysis_midline as (
    select
        d.city_mid as city,
        d.donor_mid as donor,
        d.pm_name_mid as pm_name,
        d.fellow_name_mid as fellow_name,
        d.student_grade_mid as grade,
        f.math_learning_level_status_midline_mid as math_level,
        count(distinct f.student_id) as student_count_mid,
        count(distinct case when d.cohort_mid = '2024' then f.student_id end) as cohort_2024_count_mid,
        count(distinct case when d.cohort_mid = '2025' then f.student_id end) as cohort_2025_count_mid
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as d
        on f.student_id = d.student_id
    where d.midline_attendence = True
    group by d.city_mid, d.student_grade_mid, d.donor_mid, d.pm_name_mid, d.fellow_name_mid, f.math_learning_level_status_midline_mid
),

-- math_analysis_endline as (
--     select
--         d.city_end as city,
--         d.grade_taught_end as grade,
--         f.math_level_end as math_level,
--         count(distinct f.student_id) as student_count_end
--     from 
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
--         on f.student_id = d.student_id
--     where d.endline_attendence = True
--     group by d.city_end, d.grade_taught_end, f.math_level_end
-- ),

all_combinations as (
    select distinct
        city,
        grade,
        donor,
        pm_name,
        fellow_name,
        math_level
    from math_analysis_baseline
    
    union
    
    select distinct
        city,
        grade,
        donor,
        pm_name,
        fellow_name,
        math_level
    from math_analysis_midline
    
    -- union
    
    -- select distinct
    --     city,
    --     grade,
    --     math_level
    -- from math_analysis_endline
)

select 
    ac.city,
    ac.grade,
    ac.donor,
    ac.pm_name,
    ac.fellow_name,
    ac.math_level,
    b.student_count_base,
    b.cohort_2024_count_base,
    b.cohort_2025_count_base,
    m.student_count_mid,
    m.cohort_2024_count_mid,
    m.cohort_2025_count_mid
    -- e.student_count_end
from all_combinations as ac
left join math_analysis_baseline as b
    on
        ac.city = b.city 
        and ac.math_level = b.math_level 
        and ac.grade = b.grade
        and ac.donor = b.donor
        and ac.pm_name = b.pm_name
        and ac.fellow_name = b.fellow_name
left join math_analysis_midline as m
    on
        ac.city = m.city 
        and ac.math_level = m.math_level 
        and ac.grade = m.grade
        and ac.donor = m.donor
        and ac.pm_name = m.pm_name
        and ac.fellow_name = m.fellow_name
-- left join math_analysis_endline e
--     on ac.city = e.city 
--     and ac.math_level = e.math_level 
--     and ac.grade = e.grade
order by ac.city, ac.grade, ac.math_level
