with RF_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.STUDENT_GRADE_BASE as GRADE,
        F.RF_LEVEL_BASELINE_BASE as RF_STATUS,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_BASE
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, D.STUDENT_GRADE_BASE, F.RF_LEVEL_BASELINE_BASE
),

RF_analysis_midline as (
    select
        d.city_mid as city,
        d.student_grade_mid as grade,
        f.RF_level_midline_mid as RF_status,
        count(distinct f.student_id) as student_count_mid
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as d
        on f.student_id = d.student_id
    where d.midline_attendence = True
    group by d.city_mid, d.student_grade_mid, f.RF_level_midline_mid
),

-- RF_analysis_endline as (
--     select
--         d.city_end as city,
--         d.grade_taught_end as grade,
--         f.RF_status_end as RF_status,
--         count(distinct f.student_id) as student_count_end
--     from 
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
--         on f.student_id = d.student_id
--     where d.endline_attendence = True
--     group by d.city_end, d.grade_taught_end, f.RF_status_end
-- ),

ALL_COMBINATIONS as (
    select distinct
        CITY,
        GRADE,
        RF_STATUS
    from RF_ANALYSIS_BASELINE
    
    union
    
    select distinct
        city,
        grade,
        RF_status
    from RF_analysis_midline
    
    -- union
    
    -- select distinct
    --     city,
    --     grade,
    --     RF_status
    -- from RF_analysis_endline
)

select 
    AC.CITY,
    AC.GRADE,
    AC.RF_STATUS,
    B.STUDENT_COUNT_BASE,
    m.student_count_mid
    -- e.student_count_end
from ALL_COMBINATIONS as AC
left join RF_ANALYSIS_BASELINE as B
    on
        AC.CITY = B.CITY 
        and AC.RF_STATUS = B.RF_STATUS 
        and AC.GRADE = B.GRADE
left join RF_analysis_midline m
    on ac.city = m.city 
    and ac.RF_status = m.RF_status 
    and ac.grade = m.grade
-- left join RF_analysis_endline e
--     on ac.city = e.city 
--     and ac.RF_status = e.RF_status 
--     and ac.grade = e.grade
order by ac.city, ac.grade, ac.RF_status
