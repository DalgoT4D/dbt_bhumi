with RC_analysis_baseline as (
    select
        d.city_base as city,
        d.grade_taught_base as grade,
        f.RC_status_base as RC_status,
        count(distinct f.student_id) as student_count_base
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, f.RC_status_base, d.grade_taught_base
),

RC_analysis_midline as (
    select
        d.city_mid as city,
        d.grade_taught_mid as grade,
        f.RC_status_mid as RC_status,
        count(distinct f.student_id) as student_count_mid
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.midline_attendence = True
    group by d.city_mid, f.RC_status_mid, d.grade_taught_mid
),

RC_analysis_endline as (
    select
        d.city_end as city,
        d.grade_taught_end as grade,
        f.RC_status_end as RC_status,
        count(distinct f.student_id) as student_count_end
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.endline_attendence = True
    group by d.city_end, f.RC_status_end, d.grade_taught_end
),

all_combinations as (
    select distinct
        city,
        grade,
        RC_status
    from RC_analysis_baseline
    
    union
    
    select distinct
        city,
        grade,
        RC_status
    from RC_analysis_midline
    
    union
    
    select distinct
        city,
        grade,
        RC_status
    from RC_analysis_endline
)

select 
    ac.city,
    ac.grade,
    ac.RC_status,
    b.student_count_base,
    m.student_count_mid,
    e.student_count_end
from all_combinations ac
left join RC_analysis_baseline b
    on ac.city = b.city 
    and ac.RC_status = b.RC_status 
    and ac.grade = b.grade
left join RC_analysis_midline m
    on ac.city = m.city 
    and ac.RC_status = m.RC_status 
    and ac.grade = m.grade
left join RC_analysis_endline e
    on ac.city = e.city 
    and ac.RC_status = e.RC_status 
    and ac.grade = e.grade