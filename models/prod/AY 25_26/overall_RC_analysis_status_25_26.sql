with RC_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.STUDENT_GRADE_BASE as GRADE,
        F.RC_LEARNING_LEVEL_STATUS_BASELINE_BASE as RC_STATUS,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_BASE
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, F.RC_LEARNING_LEVEL_STATUS_BASELINE_BASE, D.STUDENT_GRADE_BASE
),

-- RC_analysis_midline as (
--     select
--         d.city_mid as city,
--         d.grade_taught_mid as grade,
--         f.RC_status_mid as RC_status,
--         count(distinct f.student_id) as student_count_mid
--     from 
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
--         on f.student_id = d.student_id
--     where d.midline_attendence = True
--     group by d.city_mid, f.RC_status_mid, d.grade_taught_mid
-- ),

-- RC_analysis_endline as (
--     select
--         d.city_end as city,
--         d.grade_taught_end as grade,
--         f.RC_status_end as RC_status,
--         count(distinct f.student_id) as student_count_end
--     from 
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
--         on f.student_id = d.student_id
--     where d.endline_attendence = True
--     group by d.city_end, f.RC_status_end, d.grade_taught_end
-- ),

ALL_COMBINATIONS as (
    select distinct
        CITY,
        GRADE,
        RC_STATUS
    from RC_ANALYSIS_BASELINE
    
    -- union
    
    -- select distinct
    --     city,
    --     grade,
    --     RC_status
    -- from RC_analysis_midline
    
    -- union
    
    -- select distinct
    --     city,
    --     grade,
    --     RC_status
    -- from RC_analysis_endline
)

select 
    AC.CITY,
    AC.GRADE,
    AC.RC_STATUS,
    B.STUDENT_COUNT_BASE
    -- m.student_count_mid,
    -- e.student_count_end
from ALL_COMBINATIONS as AC
left join RC_ANALYSIS_BASELINE as B
    on
        AC.CITY = B.CITY 
        and AC.RC_STATUS = B.RC_STATUS 
        and AC.GRADE = B.GRADE
-- left join RC_analysis_midline m
--     on ac.city = m.city 
--     and ac.RC_status = m.RC_status 
--     and ac.grade = m.grade
-- left join RC_analysis_endline e
--     on ac.city = e.city 
--     and ac.RC_status = e.RC_status 
--     and ac.grade = e.grade
