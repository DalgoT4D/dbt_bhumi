with assessment_completion_baseline as (
    select
        d.city_base as city,
        d.student_grade_base as grade,
        count(distinct f.student_id) as total_students_base,
        count(distinct case when f.rc_level_baseline_base in ('','Not Assessed') then f.student_id end) as unassessed_students_rc_base,
        count(distinct case when f.rf_level_baseline_base in ('','Not Assessed') then f.student_id end) as unassessed_students_rf_base,
        count(distinct case when f.math_level_baseline_base in ('','Not Assessed') then f.student_id end) as unassessed_students_math_base
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, d.student_grade_base
),

assessment_completion_midline as (
    select
        d.city_mid as city,
        d.student_grade_mid as grade,
        count(distinct f.student_id) as total_students_mid,
        count(distinct case when f.rc_level_midline_mid in ('','Not Assessed') then f.student_id end) as unassessed_students_rc_mid,
        count(distinct case when f.rf_level_midline_mid in ('','Not Assessed') then f.student_id end) as unassessed_students_rf_mid,
        count(distinct case when f.math_level_midline_mid in ('','Not Assessed') then f.student_id end) as unassessed_students_math_mid
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as d
        on f.student_id = d.student_id
    where d.midline_attendence = True
    group by d.city_mid, d.student_grade_mid
),

all_combinations as (
    select distinct
        city,
        grade
    from assessment_completion_baseline
    
    union
    
    select distinct
        city,
        grade
    from assessment_completion_midline
    
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
    m.total_students_mid,
    -- e.total_students_end,

    -- Reading Comprehension
    coalesce(b.total_students_base - b.unassessed_students_rc_base, 0) as assessed_students_rc_base,
    1 - coalesce(((b.unassessed_students_rc_base)::numeric / nullif(b.total_students_base, 0)), 0) as perc_comp_rc_base,
    coalesce(m.total_students_mid - m.unassessed_students_rc_mid, 0) as assessed_students_rc_mid,
    1 - coalesce(((m.unassessed_students_rc_mid)::numeric / nullif(m.total_students_mid, 0)), 0) as perc_comp_rc_mid,
    -- coalesce(e.total_students_end - e.unassessed_students_RC_end, 0) as assessed_students_RC_end,
    -- 1 - coalesce(((e.unassessed_students_RC_end)::numeric / nullif(e.total_students_end, 0)), 0) as perc_comp_RC_end,

    -- Reading Fluency
    coalesce(b.total_students_base - b.unassessed_students_rf_base, 0) as assessed_students_rf_base,
    1 - coalesce(((b.unassessed_students_rf_base)::numeric / nullif(b.total_students_base, 0)), 0) as perc_comp_rf_base,
    coalesce(m.total_students_mid - m.unassessed_students_rf_mid, 0) as assessed_students_rf_mid,
    1 - coalesce(((m.unassessed_students_rf_mid)::numeric / nullif(m.total_students_mid, 0)), 0) as perc_comp_rf_mid,
    -- coalesce(e.total_students_end - e.unassessed_students_RF_end, 0) as assessed_students_RF_end,
    -- 1 - coalesce(((e.unassessed_students_RF_end)::numeric / nullif(e.total_students_end, 0)), 0) as perc_comp_RF_end,

    -- Math
    coalesce(b.total_students_base - b.unassessed_students_math_base, 0) as assessed_students_math_base,
    1 - coalesce(((b.unassessed_students_math_base)::numeric / nullif(b.total_students_base, 0)), 0) as perc_comp_math_base,
    coalesce(m.total_students_mid - m.unassessed_students_math_mid, 0) as assessed_students_math_mid,
    1 - coalesce(((m.unassessed_students_math_mid)::numeric / nullif(m.total_students_mid, 0)), 0) as perc_comp_math_mid
    -- coalesce(e.total_students_end - e.unassessed_students_math_end, 0) as assessed_students_math_end,
    -- 1 - coalesce(((e.unassessed_students_math_end)::numeric / nullif(e.total_students_end, 0)), 0) as perc_comp_math_end

from all_combinations as ac
left join assessment_completion_baseline as b
    on
        ac.city = b.city 
        and ac.grade = b.grade
left join assessment_completion_midline as m
    on
        ac.city = m.city 
        and ac.grade = m.grade
-- left join assessment_completion_endline e
--     on ac.city = e.city 
--     and ac.grade = e.grade
order by ac.city, ac.grade
