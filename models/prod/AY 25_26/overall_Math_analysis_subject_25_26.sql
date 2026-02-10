with math_analysis_baseline as (
    select
        d.city_base as city,
        d.student_grade_base as grade,
        f.math_level_baseline_base as math_level,
        d.fellow_name_base as fellow_name,
        count(distinct f.student_id) as student_count_base,
        avg(f.final_baseline_level_mastery_base) as avg_mastery_base,
        avg(f.baseline_numbers_base) as avg_perc_numbers_base,
        avg(f.baseline_patterns_base) as avg_perc_patterns_base,
        avg(f.baseline_geometry_base) as avg_perc_geometry_base,
        avg(f.baseline_total_in_mensuration_base) as avg_perc_mensuration_base,
        avg(f.baseline_total_in_time_base) as avg_perc_time_base,
        avg(f.baseline_total_in_operations_base) as avg_perc_operations_base,
        avg(f.baseline_total_in_data_base) as avg_perc_data_handling_base
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, d.student_grade_base, f.math_level_baseline_base, d.fellow_name_base
),

math_analysis_midline as (
    select
        d.city_mid as city,
        d.student_grade_mid as grade,
        f.math_level_midline_mid as math_level,
        d.fellow_name_mid as fellow_name,
        count(distinct f.student_id) as student_count_mid,
        avg(f.final_midline_level_mastery_mid) as avg_mastery_mid,
        avg(f.midline_numbers_mid) as avg_perc_numbers_mid,
        avg(f.midline_patterns_mid) as avg_perc_patterns_mid,
        avg(f.midline_geometry_mid) as avg_perc_geometry_mid,
        avg(f.midline_total_in_mensuration_mid) as avg_perc_mensuration_mid,
        avg(f.midline_total_in_time_mid) as avg_perc_time_mid,
        avg(f.midline_total_in_operations_mid) as avg_perc_operations_mid,
        avg(f.midline_total_in_data_mid) as avg_perc_data_handling_mid
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as d
        on f.student_id = d.student_id
    where d.midline_attendence = True
    group by d.city_mid, d.student_grade_mid, f.math_level_midline_mid, d.fellow_name_mid
),

-- math_analysis_endline as (
--     select
--         d.city_end as city,
--         d.grade_taught_end as grade,
--         avg(f.math_mastery_end) as avg_mastery_end,
--         avg(f.math_perc_numbers_end) as avg_perc_mastery_numbers_end,
--         avg(f.math_perc_patterns_end) as avg_perc_mastery_patterns_end,
--         avg(f.math_perc_geometry_end) as avg_perc_mastery_geometry_end,
--         avg(f.math_perc_mensuration_end) as avg_perc_mastery_mensuration_end,
--         avg(f.math_perc_time_end) as avg_perc_mastery_time_end,
--         avg(f.math_perc_operations_end) as avg_perc_mastery_operations_end,
--         avg(f.math_perc_data_handling_end) as avg_perc_mastery_data_handling_end
--     from 
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
--         on f.student_id = d.student_id
--     where d.endline_attendence = True
--     group by d.city_end, d.grade_taught_end
-- ),

all_combinations as (
    select distinct
        city,
        grade,
        math_level,
        fellow_name
    from math_analysis_baseline
    
    union
    
    select distinct
        city,
        grade,
        math_level,
        fellow_name
    from math_analysis_midline
    
    -- union
    
    -- select distinct
    --     city,
    --     grade
    -- from math_analysis_endline
)

select 
    ac.city,
    ac.grade,
    ac.math_level,
    ac.fellow_name,
    
    b.student_count_base,
    m.student_count_mid,
    -- Baseline scores
    b.avg_mastery_base,
    b.avg_perc_numbers_base,
    b.avg_perc_patterns_base,
    b.avg_perc_geometry_base,
    b.avg_perc_mensuration_base,
    b.avg_perc_time_base,
    b.avg_perc_operations_base,
    b.avg_perc_data_handling_base,
    -- Midline scores
    m.avg_mastery_mid,
    m.avg_perc_numbers_mid,
    m.avg_perc_patterns_mid,
    m.avg_perc_geometry_mid,
    m.avg_perc_mensuration_mid,
    m.avg_perc_time_mid,
    m.avg_perc_operations_mid,
    m.avg_perc_data_handling_mid
    -- -- Endline scores
    -- e.avg_mastery_end,
    -- e.avg_perc_mastery_numbers_end,
    -- e.avg_perc_mastery_patterns_end,
    -- e.avg_perc_mastery_geometry_end,
    -- e.avg_perc_mastery_mensuration_end,
    -- e.avg_perc_mastery_time_end,
    -- e.avg_perc_mastery_operations_end,
    -- e.avg_perc_mastery_data_handling_end
    
from all_combinations as ac
left join math_analysis_baseline as b
    on
        ac.city = b.city 
        and ac.grade = b.grade
        and ac.math_level = b.math_level
        and ac.fellow_name = b.fellow_name
left join math_analysis_midline as m
    on
        ac.city = m.city 
        and ac.grade = m.grade
        and ac.math_level = m.math_level
        and ac.fellow_name = m.fellow_name
-- left join math_analysis_endline e
--     on ac.city = e.city 
--     and ac.grade = e.grade
order by ac.city, ac.grade
