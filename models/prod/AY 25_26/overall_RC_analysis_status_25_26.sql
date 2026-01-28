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

RC_ANALYSIS_MIDLINE as (
    select
        D.CITY_MID as CITY,
        D.STUDENT_GRADE_MID as GRADE,
        F.RC_LEARNING_LEVEL_STATUS_MIDLINE_MID as RC_STATUS,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_MID
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.MIDLINE_ATTENDENCE = True
    group by D.CITY_MID, D.STUDENT_GRADE_MID, F.RC_LEARNING_LEVEL_STATUS_MIDLINE_MID
),

ALL_COMBINATIONS as (
    select distinct
        CITY,
        GRADE,
        RC_STATUS
    from RC_ANALYSIS_BASELINE
    
    union
    
    select distinct
        CITY,
        GRADE,
        RC_STATUS
    from RC_ANALYSIS_MIDLINE
    
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
    B.STUDENT_COUNT_BASE,
    M.STUDENT_COUNT_MID
    -- e.student_count_end
from ALL_COMBINATIONS as AC
left join RC_ANALYSIS_BASELINE as B
    on
        AC.CITY = B.CITY 
        and AC.RC_STATUS = B.RC_STATUS 
        and AC.GRADE = B.GRADE
left join RC_ANALYSIS_MIDLINE as M
    on
        AC.CITY = M.CITY 
        and AC.RC_STATUS = M.RC_STATUS 
        and AC.GRADE = M.GRADE
-- left join RC_analysis_endline e
--     on ac.city = e.city 
--     and ac.RC_status = e.RC_status 
--     and ac.grade = e.grade
