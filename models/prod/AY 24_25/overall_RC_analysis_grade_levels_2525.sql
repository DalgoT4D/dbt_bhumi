with RC_analysis_baseline as (
    select
        d.city_base,
        f.RC_grade_level_base,
        count(distinct f.student_id) as student_count_base
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, f.RC_grade_level_base
),

RC_analysis_midline as (
    select
        d.city_mid,
        f.RC_grade_level_mid,
        count(distinct f.student_id) as student_count_mid
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.midline_attendence = True
    group by d.city_mid, f.RC_grade_level_mid
),

RC_analysis_endline as (
    select
        d.city_end,
        f.RC_grade_level_end,
        count(distinct f.student_id) as student_count_end
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.endline_attendence = True
    group by d.city_end, f.RC_grade_level_end
)

select
    coalesce(b.city_base, m.city_mid, e.city_end) as city,
    coalesce(b.RC_grade_level_base, m.RC_grade_level_mid, e.RC_grade_level_end) as RC_grade_level,
    b.student_count_base,
    m.student_count_mid,
    e.student_count_end

from RC_analysis_baseline b
full outer join RC_analysis_midline m
    on b.city_base = m.city_mid and b.RC_grade_level_base = m.RC_grade_level_mid
full outer join RC_analysis_endline e
    on b.city_base = e.city_end and b.RC_grade_level_base = e.RC_grade_level_end