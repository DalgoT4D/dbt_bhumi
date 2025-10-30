with assessment_completion_baseline as (
    select
        d.city_base as city,
        d.student_grade_base as grade,
        count(distinct f.student_id) as total_students_base,
        count(distinct case when f.rc_level_baseline_base in ('','Not Assessed') then f.student_id end) as unassessed_students_RC_base,
        count(distinct case when f.rf_level_baseline_base in ('','Not Assessed') then f.student_id end) as unassessed_students_RF_base,
        count(distinct case when f.math_level_baseline_base in ('','Not Assessed') then f.student_id end) as unassessed_students_math_base
    from 
        {{ref('base_mid_end_comb_scores_25_26_fct')}} f
        inner join {{ref('base_mid_end_comb_students_25_26_dim')}} d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, d.student_grade_base
),

-- assessment_completion_midline as (
--     select
--         d.city_mid as city,
--         d.grade_taught_mid as grade,
--         count(distinct f.student_id) as total_students_mid,
--         count(distinct case when f.RC_level_mid in ('','Not Assessed') then f.student_id end) as unassessed_students_RC_mid,
--         count(distinct case when f.RF_status_mid in ('','Not Assessed') then f.student_id end) as unassessed_students_RF_mid,
--         count(distinct case when f.math_level_mid in ('','Not Assessed') then f.student_id end) as unassessed_students_math_mid
--     from 
--         {{ref('base_mid_end_comb_scores_2425_fct')}} f
--         inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
--         on f.student_id = d.student_id
--     where d.midline_attendence = True
--     group by d.city_mid, d.grade_taught_mid
-- ),

-- assessment_completion_endline as (
--     select
--         d.city_end as city,
--         d.grade_taught_end as grade,
--         count(distinct f.student_id) as total_students_end,
--         count(distinct case when f.RC_level_end in ('','Not Assessed') then f.student_id end) as unassessed_students_RC_end,
--         count(distinct case when f.RF_status_end in ('','Not Assessed') then f.student_id end) as unassessed_students_RF_end,
--         count(distinct case when f.math_level_end in ('','Not Assessed') then f.student_id end) as unassessed_students_math_end
--     from 
--         {{ref('base_mid_end_comb_scores_2425_fct')}} f
--         inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
--         on f.student_id = d.student_id
--     where d.endline_attendence = True
--     group by d.city_end, d.grade_taught_end
-- ),

all_combinations as (
    select distinct
        city,
        grade
    from assessment_completion_baseline
    
    -- union
    
    -- select distinct
    --     city,
    --     grade
    -- from assessment_completion_midline
    
    -- union
    
    -- select distinct
    --     city,
    --     grade
    -- from assessment_completion_endline
)

select 
    ac.city,
    ac.grade,
    b.total_students_base,
    -- m.total_students_mid,
    -- e.total_students_end,

    -- Reading Comprehension
    coalesce(b.total_students_base - b.unassessed_students_RC_base, 0) as assessed_students_RC_base,
    1 - coalesce(((b.unassessed_students_RC_base)::numeric / nullif(b.total_students_base, 0)), 0) as perc_comp_RC_base,
    -- coalesce(m.total_students_mid - m.unassessed_students_RC_mid, 0) as assessed_students_RC_mid,
    -- 1 - coalesce(((m.unassessed_students_RC_mid)::numeric / nullif(m.total_students_mid, 0)), 0) as perc_comp_RC_mid,
    -- coalesce(e.total_students_end - e.unassessed_students_RC_end, 0) as assessed_students_RC_end,
    -- 1 - coalesce(((e.unassessed_students_RC_end)::numeric / nullif(e.total_students_end, 0)), 0) as perc_comp_RC_end,

    -- Reading Fluency
    coalesce(b.total_students_base - b.unassessed_students_RF_base, 0) as assessed_students_RF_base,
    1 - coalesce(((b.unassessed_students_RF_base)::numeric / nullif(b.total_students_base, 0)), 0) as perc_comp_RF_base,
    -- coalesce(m.total_students_mid - m.unassessed_students_RF_mid, 0) as assessed_students_RF_mid,
    -- 1 - coalesce(((m.unassessed_students_RF_mid)::numeric / nullif(m.total_students_mid, 0)), 0) as perc_comp_RF_mid,
    -- coalesce(e.total_students_end - e.unassessed_students_RF_end, 0) as assessed_students_RF_end,
    -- 1 - coalesce(((e.unassessed_students_RF_end)::numeric / nullif(e.total_students_end, 0)), 0) as perc_comp_RF_end,

    -- Math
    coalesce(b.total_students_base - b.unassessed_students_math_base, 0) as assessed_students_math_base,
    1 - coalesce(((b.unassessed_students_math_base)::numeric / nullif(b.total_students_base, 0)), 0) as perc_comp_math_base
    -- coalesce(m.total_students_mid - m.unassessed_students_math_mid, 0) as assessed_students_math_mid,
    -- 1 - coalesce(((m.unassessed_students_math_mid)::numeric / nullif(m.total_students_mid, 0)), 0) as perc_comp_math_mid,
    -- coalesce(e.total_students_end - e.unassessed_students_math_end, 0) as assessed_students_math_end,
    -- 1 - coalesce(((e.unassessed_students_math_end)::numeric / nullif(e.total_students_end, 0)), 0) as perc_comp_math_end

from all_combinations ac
left join assessment_completion_baseline b
    on ac.city = b.city 
    and ac.grade = b.grade
-- left join assessment_completion_midline m
--     on ac.city = m.city 
--     and ac.grade = m.grade
-- left join assessment_completion_endline e
--     on ac.city = e.city 
--     and ac.grade = e.grade
-- order by ac.city, ac.grade