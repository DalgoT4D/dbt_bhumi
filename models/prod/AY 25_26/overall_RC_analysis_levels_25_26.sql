with RC_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.STUDENT_GRADE_BASE as GRADE,
        F.RC_LEVEL_BASELINE_BASE as RC_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_BASE
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, D.STUDENT_GRADE_BASE, F.RC_LEVEL_BASELINE_BASE
),

-- RC_analysis_midline as (
--     select
--         d.city_mid as city,
--         d.grade_taught_mid as grade,
--         f.RC_level_mid as RC_level,
--         count(distinct f.student_id) as student_count_mid
--     from 
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
--         on f.student_id = d.student_id
--     where d.midline_attendence = True
--     group by d.city_mid, d.grade_taught_mid, f.RC_level_mid
-- ),

-- RC_analysis_endline as (
--     select
--         d.city_end as city,
--         d.grade_taught_end as grade,
--         f.RC_level_end as RC_level,
--         count(distinct f.student_id) as student_count_end
--     from 
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
--         on f.student_id = d.student_id
--     where d.endline_attendence = True
--     group by d.city_end, d.grade_taught_end, f.RC_level_end
-- ),

ALL_COMBINATIONS as (
    select distinct
        CITY,
        GRADE,
        RC_LEVEL
    from RC_ANALYSIS_BASELINE
    
    -- union
    
    -- select distinct
    --     city,
    --     grade,
    --     RC_level
    -- from RC_analysis_midline
    
    -- union
    
    -- select distinct
    --     city,
    --     grade,
    --     RC_level
    -- from RC_analysis_endline
)

select 
    AC.CITY,
    AC.GRADE,
    AC.RC_LEVEL,
    B.STUDENT_COUNT_BASE
    -- m.student_count_mid,
    -- e.student_count_end
from ALL_COMBINATIONS as AC
left join RC_ANALYSIS_BASELINE as B
    on
        AC.CITY = B.CITY 
        and AC.RC_LEVEL = B.RC_LEVEL 
        and AC.GRADE = B.GRADE
-- left join RC_analysis_midline m
--     on ac.city = m.city 
--     and ac.RC_level = m.RC_level 
--     and ac.grade = m.grade
-- left join RC_analysis_endline e
--     on ac.city = e.city 
--     and ac.RC_level = e.RC_level 
--     and ac.grade = e.grade
-- order by ac.city, ac.grade, ac.RC_level
