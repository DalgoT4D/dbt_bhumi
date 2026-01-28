with RC_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.STUDENT_GRADE_BASE as GRADE,
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
    group by D.CITY_BASE, D.STUDENT_GRADE_BASE
),

RC_ANALYSIS_MIDLINE as (
    select
        D.CITY_MID as CITY,
        D.STUDENT_GRADE_MID as GRADE,
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
    group by D.CITY_MID, D.STUDENT_GRADE_MID
),

ALL_COMBINATIONS as (
    select distinct
        CITY,
        GRADE
    from RC_ANALYSIS_BASELINE
    
    union
    
    select distinct
        CITY,
        GRADE
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
left join RC_ANALYSIS_MIDLINE as M
    on
        AC.CITY = M.CITY 
        and AC.GRADE = M.GRADE
-- left join RC_analysis_endline e
--     on ac.city = e.city 
--     and ac.grade = e.grade
order by AC.CITY, AC.GRADE
