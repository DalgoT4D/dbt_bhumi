with assessment_completion_baseline as (
    select
        d.city_base as city,
        d.grade_taught_base as grade,
        count(distinct f.student_id) as total_students_base,
        count(distinct case when f.rc_level_base in ('','Not Assessed') then f.student_id end) as unassessed_students_rc_base,
        count(distinct case when f.rf_status_base in ('','Not Assessed') then f.student_id end) as unassessed_students_rf_base,
        count(distinct case when f.math_level_base in ('','Not Assessed') then f.student_id end) as unassessed_students_math_base
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, d.grade_taught_base
),

assessment_completion_midline as (
    select
        d.city_mid as city,
        d.grade_taught_mid as grade,
        count(distinct f.student_id) as total_students_mid,
        count(distinct case when f.rc_level_mid in ('','Not Assessed') then f.student_id end) as unassessed_students_rc_mid,
        count(distinct case when f.rf_status_mid in ('','Not Assessed') then f.student_id end) as unassessed_students_rf_mid,
        count(distinct case when f.math_level_mid in ('','Not Assessed') then f.student_id end) as unassessed_students_math_mid
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as d
        on f.student_id = d.student_id
    where d.midline_attendence = True
    group by d.city_mid, d.grade_taught_mid
),

assessment_completion_endline as (
    select
        d.city_end as city,
        d.grade_taught_end as grade,
        count(distinct f.student_id) as total_students_end,
        count(distinct case when f.rc_level_end in ('','Not Assessed') then f.student_id end) as unassessed_students_rc_end,
        count(distinct case when f.rf_status_end in ('','Not Assessed') then f.student_id end) as unassessed_students_rf_end,
        count(distinct case when f.math_level_end in ('','Not Assessed') then f.student_id end) as unassessed_students_math_end
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as d
        on f.student_id = d.student_id
    where d.endline_attendence = True
    group by d.city_end, d.grade_taught_end
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
    
    union
    
    select distinct
        city,
        grade
    from assessment_completion_endline
)

select 
    ac.city,
    ac.grade,
    b.total_students_base,
    m.total_students_mid,
    e.total_students_end,

    -- Reading Comprehension
    coalesce(b.total_students_base - b.unassessed_students_rc_base, 0) as assessed_students_rc_base,
    round((1 - coalesce(((b.unassessed_students_rc_base)::numeric / nullif(b.total_students_base, 0)), 0)) * 100, 2) as perc_comp_rc_base,
    coalesce(m.total_students_mid - m.unassessed_students_rc_mid, 0) as assessed_students_rc_mid,
    round((1 - coalesce(((m.unassessed_students_rc_mid)::numeric / nullif(m.total_students_mid, 0)), 0)) * 100, 2) as perc_comp_rc_mid,
    coalesce(e.total_students_end - e.unassessed_students_rc_end, 0) as assessed_students_rc_end,
    round((1 - coalesce(((e.unassessed_students_rc_end)::numeric / nullif(e.total_students_end, 0)), 0)) * 100, 2) as perc_comp_rc_end,

    -- Reading Fluency
    coalesce(b.total_students_base - b.unassessed_students_rf_base, 0) as assessed_students_rf_base,
    round((1 - coalesce(((b.unassessed_students_rf_base)::numeric / nullif(b.total_students_base, 0)), 0)) * 100, 2) as perc_comp_rf_base,
    coalesce(m.total_students_mid - m.unassessed_students_rf_mid, 0) as assessed_students_rf_mid,
    round((1 - coalesce(((m.unassessed_students_rf_mid)::numeric / nullif(m.total_students_mid, 0)), 0)) * 100, 2) as perc_comp_rf_mid,
    coalesce(e.total_students_end - e.unassessed_students_rf_end, 0) as assessed_students_rf_end,
    round((1 - coalesce(((e.unassessed_students_rf_end)::numeric / nullif(e.total_students_end, 0)), 0)) * 100, 2) as perc_comp_rf_end,

    -- Math
    coalesce(b.total_students_base - b.unassessed_students_math_base, 0) as assessed_students_math_base,
    round((1 - coalesce(((b.unassessed_students_math_base)::numeric / nullif(b.total_students_base, 0)), 0)) * 100, 2) as perc_comp_math_base,
    coalesce(m.total_students_mid - m.unassessed_students_math_mid, 0) as assessed_students_math_mid,
    round((1 - coalesce(((m.unassessed_students_math_mid)::numeric / nullif(m.total_students_mid, 0)), 0)) * 100, 2) as perc_comp_math_mid,
    coalesce(e.total_students_end - e.unassessed_students_math_end, 0) as assessed_students_math_end,
    round((1 - coalesce(((e.unassessed_students_math_end)::numeric / nullif(e.total_students_end, 0)), 0)) * 100, 2) as perc_comp_math_end

from all_combinations as ac
left join assessment_completion_baseline as b
    on
        ac.city = b.city 
        and ac.grade = b.grade
left join assessment_completion_midline as m
    on
        ac.city = m.city 
        and ac.grade = m.grade
left join assessment_completion_endline as e
    on
        ac.city = e.city 
        and ac.grade = e.grade
order by ac.city, ac.grade
