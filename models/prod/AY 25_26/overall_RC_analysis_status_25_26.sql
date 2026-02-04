with RC_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.STUDENT_GRADE_BASE as GRADE,
        D.DONOR_BASE as DONOR,
        D.PM_NAME_BASE as PM_NAME,
        F.RC_LEARNING_LEVEL_STATUS_BASELINE_BASE as RC_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_BASE,
        count(distinct case when D.COHORT_BASE = '2024' then F.STUDENT_ID end) as COHORT_2024_COUNT_BASE,
        count(distinct case when D.COHORT_BASE = '2025' then F.STUDENT_ID end) as COHORT_2025_COUNT_BASE
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, F.RC_LEARNING_LEVEL_STATUS_BASELINE_BASE, D.STUDENT_GRADE_BASE, D.DONOR_BASE, D.PM_NAME_BASE
),

RC_ANALYSIS_MIDLINE as (
    select
        D.CITY_MID as CITY,
        D.STUDENT_GRADE_MID as GRADE,
        D.DONOR_MID as DONOR,
        D.PM_NAME_MID as PM_NAME,
        F.RC_LEARNING_LEVEL_STATUS_MIDLINE_MID as RC_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_MID,
        count(distinct case when D.COHORT_MID = '2024' then F.STUDENT_ID end) as COHORT_2024_COUNT_MID,
        count(distinct case when D.COHORT_MID = '2025' then F.STUDENT_ID end) as COHORT_2025_COUNT_MID
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.MIDLINE_ATTENDENCE = True
    group by D.CITY_MID, D.STUDENT_GRADE_MID, D.DONOR_MID, D.PM_NAME_MID, F.RC_LEARNING_LEVEL_STATUS_MIDLINE_MID
),

ALL_COMBINATIONS as (
    select distinct
        CITY,
        GRADE,
        DONOR,
        PM_NAME,
        RC_LEVEL
    from RC_ANALYSIS_BASELINE
    
    union
    
    select distinct
        CITY,
        GRADE,
        DONOR,
        PM_NAME,
        RC_LEVEL
    from RC_ANALYSIS_MIDLINE
    
    -- union
    
    -- select distinct
    --     city,
    --     grade,
    --     RC_LEVEL
    -- from RC_analysis_endline
)

select 
    AC.CITY,
    AC.GRADE,
    AC.DONOR,
    AC.PM_NAME,
    AC.RC_LEVEL,
    B.STUDENT_COUNT_BASE,
    B.COHORT_2024_COUNT_BASE,
    B.COHORT_2025_COUNT_BASE,
    M.STUDENT_COUNT_MID,
    M.COHORT_2024_COUNT_MID,
    M.COHORT_2025_COUNT_MID
    -- e.student_count_end
from ALL_COMBINATIONS as AC
left join RC_ANALYSIS_BASELINE as B
    on
        AC.CITY = B.CITY 
        and AC.RC_LEVEL = B.RC_LEVEL 
        and AC.GRADE = B.GRADE
        and AC.DONOR = B.DONOR
        and AC.PM_NAME = B.PM_NAME
left join RC_ANALYSIS_MIDLINE as M
    on
        AC.CITY = M.CITY 
        and AC.RC_LEVEL = M.RC_LEVEL 
        and AC.GRADE = M.GRADE
        and AC.DONOR = M.DONOR
        and AC.PM_NAME = M.PM_NAME
-- left join RC_analysis_endline e
--     on ac.city = e.city 
--     and ac.RC_LEVEL = e.RC_LEVEL 
--     and ac.grade = e.grade
