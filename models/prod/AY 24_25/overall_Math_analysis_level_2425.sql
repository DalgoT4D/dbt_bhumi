with math_analysis_baseline as (
    select
        d.city_base as city,
        d.grade_taught_base as grade,
        f.math_level_base as math_level,
        count(distinct f.student_id) as student_count_base
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, d.grade_taught_base, f.math_level_base
),

math_analysis_midline as (
    select
        d.city_mid as city,
        d.grade_taught_mid as grade,
        f.math_level_mid as math_level,
        count(distinct f.student_id) as student_count_mid
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.midline_attendence = True
    group by d.city_mid, d.grade_taught_mid, f.math_level_mid
),

math_analysis_endline as (
    select
        d.city_end as city,
        d.grade_taught_end as grade,
        f.math_level_end as math_level,
        count(distinct f.student_id) as student_count_end
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.endline_attendence = True
    group by d.city_end, d.grade_taught_end, f.math_level_end
),

all_combinations as (
    select distinct
        city,
        grade,
        math_level
    from math_analysis_baseline
    
    union
    
    select distinct
        city,
        grade,
        math_level
    from math_analysis_midline
    
    union
    
    select distinct
        city,
        grade,
        math_level
    from math_analysis_endline
)

select 
    ac.city,
    ac.grade,
    ac.math_level,
    b.student_count_base,
    m.student_count_mid,
    e.student_count_end
from all_combinations ac
left join math_analysis_baseline b
    on ac.city = b.city 
    and ac.math_level = b.math_level 
    and ac.grade = b.grade
left join math_analysis_midline m
    on ac.city = m.city 
    and ac.math_level = m.math_level 
    and ac.grade = m.grade
left join math_analysis_endline e
    on ac.city = e.city 
    and ac.math_level = e.math_level 
    and ac.grade = e.grade
order by ac.city, ac.grade, ac.math_level