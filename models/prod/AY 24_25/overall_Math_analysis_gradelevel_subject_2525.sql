with math_analysis_baseline as (
    select
        d.city_base,
        f.math_level_base,
        count(distinct f.student_id) as student_count_base,
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
    group by d.city_base, f.math_level_base
),

math_analysis_midline as (
    select
        d.city_mid,
        f.math_level_mid,
        count(distinct f.student_id) as student_count_mid,
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
    group by d.city_mid, f.math_level_mid
),

math_analysis_endline as (
    select
        d.city_end,
        f.math_level_end,
        count(distinct f.student_id) as student_count_end,
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
    group by d.city_end, f.math_level_end
)

select
    coalesce(b.city_base, m.city_mid, e.city_end) as city,
    coalesce(b.math_level_base, m.math_level_mid, e.math_level_end) as math_level,
---------------------------------
    b.student_count_base,
    b.avg_mastery_base,
    b.avg_perc_mastery_numbers_base,
    b.avg_perc_mastery_patterns_base,
    b.avg_perc_mastery_geometry_base,
    b.avg_perc_mastery_mensuration_base,
    b.avg_perc_mastery_time_base,
    b.avg_perc_mastery_operations_base,
    b.avg_perc_mastery_data_handling_base,
-----------------------------------
    m.student_count_mid,
    m.avg_mastery_mid,
    m.avg_perc_mastery_numbers_mid,
    m.avg_perc_mastery_patterns_mid,
    m.avg_perc_mastery_geometry_mid,
    m.avg_perc_mastery_mensuration_mid,
    m.avg_perc_mastery_time_mid,
    m.avg_perc_mastery_operations_mid,
    m.avg_perc_mastery_data_handling_mid,
-----------------------------------
    e.student_count_end,
    e.avg_mastery_end,
    e.avg_perc_mastery_numbers_end,
    e.avg_perc_mastery_patterns_end,
    e.avg_perc_mastery_geometry_end,
    e.avg_perc_mastery_mensuration_end,
    e.avg_perc_mastery_time_end,
    e.avg_perc_mastery_operations_end,
    e.avg_perc_mastery_data_handling_end
    
from math_analysis_baseline b
full outer join math_analysis_midline m
    on b.city_base = m.city_mid and b.math_level_base = m.math_level_mid
full outer join math_analysis_endline e
    on b.city_base = e.city_end and b.math_level_base = e.math_level_end