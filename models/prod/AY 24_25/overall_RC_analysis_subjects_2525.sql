with RC_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY, 
        D.GRADE_TAUGHT_BASE as GRADE,
        avg(F.FACTUAL_BASE) as FACTUAL_BASE,
        avg(F.INFERENCE_BASE) as INFERENCE_BASE,
        avg(F.CRITICAL_THINKING_BASE) as CRITICAL_THINKING_BASE,
        avg(F.VOCABULARY_BASE) as VOCABULARY_BASE,
        avg(F.GRAMMAR_BASE) as GRAMMAR_BASE
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, D.GRADE_TAUGHT_BASE
),

RC_ANALYSIS_MIDLINE as (
    select
        D.CITY_MID as CITY,
        D.GRADE_TAUGHT_MID as GRADE,
        avg(F.FACTUAL_MID) as FACTUAL_MID,
        avg(F.INFERENCE_MID) as INFERENCE_MID,
        avg(F.CRITICAL_THINKING_MID) as CRITICAL_THINKING_MID,
        avg(F.VOCABULARY_MID) as VOCABULARY_MID,
        avg(F.GRAMMAR_MID) as GRAMMAR_MID
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.MIDLINE_ATTENDENCE = True
    group by D.CITY_MID, D.GRADE_TAUGHT_MID
),

RC_ANALYSIS_ENDLINE as (
    select
        D.CITY_END as CITY,
        D.GRADE_TAUGHT_END as GRADE,
        avg(F.FACTUAL_END) as FACTUAL_END,
        avg(F.INFERENCE_END) as INFERENCE_END,
        avg(F.CRITICAL_THINKING_END) as CRITICAL_THINKING_END,
        avg(F.VOCABULARY_END) as VOCABULARY_END,
        avg(F.GRAMMAR_END) as GRAMMAR_END
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.ENDLINE_ATTENDENCE = True
    group by D.CITY_END, D.GRADE_TAUGHT_END
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
    
    union
    
    select distinct
        CITY,
        GRADE
    from RC_ANALYSIS_ENDLINE
)

select 
    AC.CITY,
    AC.GRADE,
    -- Factual scores
    B.FACTUAL_BASE,
    M.FACTUAL_MID,
    E.FACTUAL_END,
    -- Inference scores
    B.INFERENCE_BASE,
    M.INFERENCE_MID,
    E.INFERENCE_END,
    -- Critical Thinking scores
    B.CRITICAL_THINKING_BASE,
    M.CRITICAL_THINKING_MID,
    E.CRITICAL_THINKING_END,
    -- Vocabulary scores
    B.VOCABULARY_BASE,
    M.VOCABULARY_MID,
    E.VOCABULARY_END,
    -- Grammar scores
    B.GRAMMAR_BASE,
    M.GRAMMAR_MID,
    E.GRAMMAR_END
    
from ALL_COMBINATIONS as AC
left join RC_ANALYSIS_BASELINE as B
    on
        AC.CITY = B.CITY 
        and AC.GRADE = B.GRADE
left join RC_ANALYSIS_MIDLINE as M
    on
        AC.CITY = M.CITY 
        and AC.GRADE = M.GRADE
left join RC_ANALYSIS_ENDLINE as E
    on
        AC.CITY = E.CITY 
        and AC.GRADE = E.GRADE
order by AC.CITY, AC.GRADE
