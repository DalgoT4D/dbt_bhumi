with RC_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.STUDENT_GRADE_BASE as GRADE,
        D.DONOR_BASE as DONOR,
        D.PM_NAME_BASE as PM_NAME,
        D.FELLOW_NAME_BASE as FELLOW_NAME,
        F.RC_LEVEL_BASELINE_BASE as RC_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_BASE,
        avg(F.BASELINE_FACTUAL_BASE) as FACTUAL_BASE,
        avg(F.BASELINE_INFERENCE_BASE) as INFERENCE_BASE,
        avg(F.BASELINE_CRITICAL_THINKING_BASE) as CRITICAL_THINKING_BASE,
        avg(F.BASELINE_VOCABULARY_BASE) as VOCABULARY_BASE,
        avg(F.BASELINE_GRAMMAR_BASE) as GRAMMAR_BASE
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, D.STUDENT_GRADE_BASE, F.RC_LEVEL_BASELINE_BASE, D.FELLOW_NAME_BASE, D.DONOR_BASE, D.PM_NAME_BASE
),

RC_ANALYSIS_MIDLINE as (
    select
        D.CITY_MID as CITY,
        D.STUDENT_GRADE_MID as GRADE,
        D.DONOR_MID as DONOR,
        D.PM_NAME_MID as PM_NAME,
        D.FELLOW_NAME_MID as FELLOW_NAME,
        F.RC_LEVEL_MIDLINE_MID as RC_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_MID,
        avg(F.MIDLINE_FACTUAL_MID) as FACTUAL_MID,
        avg(F.MIDLINE_INFERENCE_MID) as INFERENCE_MID,
        avg(F.MIDLINE_CRITICAL_THINKING_MID) as CRITICAL_THINKING_MID,
        avg(F.MIDLINE_VOCABULARY_MID) as VOCABULARY_MID,
        avg(F.MIDLINE_GRAMMAR_MID) as GRAMMAR_MID
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.MIDLINE_ATTENDENCE = True
    group by D.CITY_MID, D.STUDENT_GRADE_MID, F.RC_LEVEL_MIDLINE_MID, D.FELLOW_NAME_MID, D.DONOR_MID, D.PM_NAME_MID
),

ALL_COMBINATIONS as (
    select distinct
        CITY,
        GRADE,
        DONOR,
        PM_NAME,
        FELLOW_NAME,
        RC_LEVEL
    from RC_ANALYSIS_BASELINE
    
    union
    
    select distinct
        CITY,
        GRADE,
        DONOR,
        PM_NAME,
        FELLOW_NAME,
        RC_LEVEL
    from RC_ANALYSIS_MIDLINE
    
    -- union
    
    -- select distinct
    --     city,
    --     grade
    -- from RC_analysis_endline
)

select 
    AC.CITY,
    AC.GRADE,
    AC.DONOR,
    AC.PM_NAME,
    AC.FELLOW_NAME,
    AC.RC_LEVEL,

    B.STUDENT_COUNT_BASE,
    M.STUDENT_COUNT_MID,
    -- Factual scores
    B.FACTUAL_BASE,
    M.FACTUAL_MID,
    -- e.factual_end,
    -- Inference scores
    B.INFERENCE_BASE,
    M.INFERENCE_MID,
    -- e.inference_end,
    -- Critical Thinking scores
    B.CRITICAL_THINKING_BASE,
    M.CRITICAL_THINKING_MID,
    -- e.critical_thinking_end,
    -- Vocabulary scores
    B.VOCABULARY_BASE,
    M.VOCABULARY_MID,
    -- e.vocabulary_end,
    -- Grammar scores
    B.GRAMMAR_BASE,
    M.GRAMMAR_MID
    -- e.grammar_end
    
from ALL_COMBINATIONS as AC
left join RC_ANALYSIS_BASELINE as B
    on
        AC.CITY = B.CITY 
        and AC.GRADE = B.GRADE
        and AC.RC_LEVEL = B.RC_LEVEL
        and AC.FELLOW_NAME = B.FELLOW_NAME
        and AC.DONOR = B.DONOR
        and AC.PM_NAME = B.PM_NAME
left join RC_ANALYSIS_MIDLINE as M
    on
        AC.CITY = M.CITY 
        and AC.GRADE = M.GRADE
        and AC.RC_LEVEL = M.RC_LEVEL
        and AC.FELLOW_NAME = M.FELLOW_NAME
        and AC.DONOR = M.DONOR
        and AC.PM_NAME = M.PM_NAME
-- left join RC_analysis_endline e
--     on ac.city = e.city 
--     and ac.grade = e.grade
order by AC.CITY, AC.GRADE
