with RF_analysis_baseline as (
    select
        d.city_base as city,
        d.grade_taught_base as grade,
        f.RF_status_base as RF_status,
        count(distinct f.student_id) as student_count_base
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, d.grade_taught_base, f.RF_status_base
),

RF_analysis_midline as (
    select
        d.city_mid as city,
        d.grade_taught_mid as grade,
        f.RF_status_mid as RF_status,
        count(distinct f.student_id) as student_count_mid
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.midline_attendence = True
    group by d.city_mid, d.grade_taught_mid, f.RF_status_mid
),

RF_analysis_endline as (
    select
        d.city_end as city,
        d.grade_taught_end as grade,
        f.RF_status_end as RF_status,
        count(distinct f.student_id) as student_count_end
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.endline_attendence = True
    group by d.city_end, d.grade_taught_end, f.RF_status_end
),

all_combinations as (
    select distinct
        city,
        grade,
        RF_status
    from RF_analysis_baseline
    
    union
    
    select distinct
        city,
        grade,
        RF_status
    from RF_analysis_midline
    
    union
    
    select distinct
        city,
        grade,
        RF_status
    from RF_analysis_endline
)

select 
    ac.city,
    ac.grade,
    ac.RF_status,
    b.student_count_base,
    m.student_count_mid,
    e.student_count_end
from all_combinations ac
left join RF_analysis_baseline b
    on ac.city = b.city 
    and ac.RF_status = b.RF_status 
    and ac.grade = b.grade
left join RF_analysis_midline m
    on ac.city = m.city 
    and ac.RF_status = m.RF_status 
    and ac.grade = m.grade
left join RF_analysis_endline e
    on ac.city = e.city 
    and ac.RF_status = e.RF_status 
    and ac.grade = e.grade
order by ac.city, ac.grade, ac.RF_status