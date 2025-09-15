with math_analysis_baseline as (
    select
        d.city_base as city,
        d.grade_taught_base as grade,
        avg(f.math_mastery_base) as avg_mastery_base,
        avg(f.math_perc_numbers_base) as avg_perc_mastery_numbers_base,
        avg(f.math_perc_patterns_base) as avg_perc_mastery_patterns_base,
        avg(f.math_perc_geometry_base) as avg_perc_mastery_geometry_base,
        avg(f.math_perc_mensuration_base) as avg_perc_mastery_mensuration_base,
        avg(f.math_perc_time_base) as avg_perc_mastery_time_base,
        avg(f.math_perc_operations_base) as avg_perc_mastery_operations_base,
        avg(f.math_perc_data_handling_base) as avg_perc_mastery_data_handling_base
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, d.grade_taught_base
),

math_analysis_midline as (
    select
        d.city_mid as city,
        d.grade_taught_mid as grade,
        avg(f.math_mastery_mid) as avg_mastery_mid,
        avg(f.math_perc_numbers_mid) as avg_perc_mastery_numbers_mid,
        avg(f.math_perc_patterns_mid) as avg_perc_mastery_patterns_mid,
        avg(f.math_perc_geometry_mid) as avg_perc_mastery_geometry_mid,
        avg(f.math_perc_mensuration_mid) as avg_perc_mastery_mensuration_mid,
        avg(f.math_perc_time_mid) as avg_perc_mastery_time_mid,
        avg(f.math_perc_operations_mid) as avg_perc_mastery_operations_mid,
        avg(f.math_perc_data_handling_mid) as avg_perc_mastery_data_handling_mid
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.midline_attendence = True
    group by d.city_mid, d.grade_taught_mid
),

math_analysis_endline as (
    select
        d.city_end as city,
        d.grade_taught_end as grade,
        avg(f.math_mastery_end) as avg_mastery_end,
        avg(f.math_perc_numbers_end) as avg_perc_mastery_numbers_end,
        avg(f.math_perc_patterns_end) as avg_perc_mastery_patterns_end,
        avg(f.math_perc_geometry_end) as avg_perc_mastery_geometry_end,
        avg(f.math_perc_mensuration_end) as avg_perc_mastery_mensuration_end,
        avg(f.math_perc_time_end) as avg_perc_mastery_time_end,
        avg(f.math_perc_operations_end) as avg_perc_mastery_operations_end,
        avg(f.math_perc_data_handling_end) as avg_perc_mastery_data_handling_end
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.endline_attendence = True
    group by d.city_end, d.grade_taught_end
),

all_combinations as (
    select distinct
        city,
        grade
    from math_analysis_baseline
    
    union
    
    select distinct
        city,
        grade
    from math_analysis_midline
    
    union
    
    select distinct
        city,
        grade
    from math_analysis_endline
)

select 
    ac.city,
    ac.grade,
    -- Baseline scores
    b.avg_mastery_base,
    b.avg_perc_mastery_numbers_base,
    b.avg_perc_mastery_patterns_base,
    b.avg_perc_mastery_geometry_base,
    b.avg_perc_mastery_mensuration_base,
    b.avg_perc_mastery_time_base,
    b.avg_perc_mastery_operations_base,
    b.avg_perc_mastery_data_handling_base,
    -- Midline scores
    m.avg_mastery_mid,
    m.avg_perc_mastery_numbers_mid,
    m.avg_perc_mastery_patterns_mid,
    m.avg_perc_mastery_geometry_mid,
    m.avg_perc_mastery_mensuration_mid,
    m.avg_perc_mastery_time_mid,
    m.avg_perc_mastery_operations_mid,
    m.avg_perc_mastery_data_handling_mid,
    -- Endline scores
    e.avg_mastery_end,
    e.avg_perc_mastery_numbers_end,
    e.avg_perc_mastery_patterns_end,
    e.avg_perc_mastery_geometry_end,
    e.avg_perc_mastery_mensuration_end,
    e.avg_perc_mastery_time_end,
    e.avg_perc_mastery_operations_end,
    e.avg_perc_mastery_data_handling_end
    
from all_combinations ac
left join math_analysis_baseline b
    on ac.city = b.city 
    and ac.grade = b.grade
left join math_analysis_midline m
    on ac.city = m.city 
    and ac.grade = m.grade
left join math_analysis_endline e
    on ac.city = e.city 
    and ac.grade = e.grade
order by ac.city, ac.grade