with demographics_baseline as (
    select
        t.city_base,
        t.grade_taught_base,
        t.cohort_base,
        t.fellow_name_base,
        t.school_name_base,
        t.pm_name_base,
        count(distinct t.student_id) as student_count_base
    from {{ ref('base_mid_end_comb_students_2425_dim') }} as t
    where t.baseline_attendence = True
    group by t.city_base, t.grade_taught_base, t.cohort_base, t.fellow_name_base, t.school_name_base, t.pm_name_base
    
),

demographics_midline as (
    select
        t.city_mid,
        t.grade_taught_mid,
        t.cohort_mid,
        t.fellow_name_mid,
        t.school_name_mid,
        t.pm_name_mid,
        count(distinct t.student_id) as student_count_mid
    from {{ ref('base_mid_end_comb_students_2425_dim') }} as t
    where t.midline_attendence = True
    group by t.city_mid, t.grade_taught_mid, t.cohort_mid, t.fellow_name_mid, t.school_name_mid, t.pm_name_mid
),

demographics_endline as (
    select
        t.city_end,
        t.grade_taught_end,
        t.cohort_end,
        t.fellow_name_end,
        t.school_name_end,
        t.pm_name_end,
        count(distinct t.student_id) as student_count_end
    from {{ ref('base_mid_end_comb_students_2425_dim') }} as t
    where t.endline_attendence = True
    group by t.city_end, t.grade_taught_end, t.cohort_end, t.fellow_name_end, t.school_name_end, t.pm_name_end
),

all_combinations as (
    select distinct
        city_base as city,
        grade_taught_base as grade,
        cohort_base as cohort,
        fellow_name_base as fellow_name,
        school_name_base as school_name,
        pm_name_base as pm_name
    from demographics_baseline
    
    union
    
    select distinct
        city_mid as city,
        grade_taught_mid as grade,
        cohort_mid as cohort,
        fellow_name_mid as fellow_name,
        school_name_mid as school_name,
        pm_name_mid as pm_name
    from demographics_midline
    
    union
    
    select distinct
        city_end as city,
        grade_taught_end as grade,
        cohort_end as cohort,
        fellow_name_end as fellow_name,
        school_name_end as school_name,
        pm_name_end as pm_name
    from demographics_endline
)

select 
    ac.city,
    ac.grade,
    ac.cohort,
    ac.fellow_name,
    ac.school_name,
    ac.pm_name,
    b.student_count_base,
    m.student_count_mid,
    e.student_count_end
from all_combinations as ac
left join demographics_baseline as b
    on
        ac.city = b.city_base 
        and ac.grade = b.grade_taught_base 
        and ac.cohort = b.cohort_base 
        and ac.fellow_name = b.fellow_name_base 
        and ac.school_name = b.school_name_base 
        and ac.pm_name = b.pm_name_base
left join demographics_midline as m
    on
        ac.city = m.city_mid 
        and ac.grade = m.grade_taught_mid 
        and ac.cohort = m.cohort_mid 
        and ac.fellow_name = m.fellow_name_mid 
        and ac.school_name = m.school_name_mid 
        and ac.pm_name = m.pm_name_mid
left join demographics_endline as e
    on
        ac.city = e.city_end 
        and ac.grade = e.grade_taught_end 
        and ac.cohort = e.cohort_end 
        and ac.fellow_name = e.fellow_name_end 
        and ac.school_name = e.school_name_end 
        and ac.pm_name = e.pm_name_end
