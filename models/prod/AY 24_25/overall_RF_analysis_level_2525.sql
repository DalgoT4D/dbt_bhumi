with RF_analysis_baseline as (
    select
        d.city_base,
        f.RF_status_base,
        count(distinct f.student_id) as student_count_base
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, f.RF_status_base
),

RF_analysis_midline as (
    select
        d.city_mid,
        f.RF_status_mid,
        count(distinct f.student_id) as student_count_mid
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.midline_attendence = True
    group by d.city_mid, f.RF_status_mid
),

RF_analysis_endline as (
    select
        d.city_end,
        f.RF_status_end,
        count(distinct f.student_id) as student_count_end
    from 
        {{ref('base_mid_end_comb_scores_2425_fct')}} f
        inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
        on f.student_id = d.student_id
    where d.endline_attendence = True
    group by d.city_end, f.RF_status_end
)

select
    coalesce(b.city_base, m.city_mid, e.city_end) as city,
    coalesce(b.RF_status_base, m.RF_status_mid, e.RF_status_end) as RF_status,
    b.student_count_base,
    m.student_count_mid,
    e.student_count_end

from RF_analysis_baseline b
full outer join RF_analysis_midline m
    on b.city_base = m.city_mid and b.RF_status_base = m.RF_status_mid
full outer join RF_analysis_endline e
    on b.city_base = e.city_end and b.RF_status_base = e.RF_status_end