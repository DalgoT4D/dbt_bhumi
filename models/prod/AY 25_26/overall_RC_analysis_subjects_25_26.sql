with RC_analysis_baseline as (
    select
        d.city_base as city,
        d.student_grade_base as grade,
        avg(f.baseline_factual_base) as factual_base,
        avg(f.baseline_inference_base) as inference_base,
        avg(f.baseline_critical_thinking_base) as critical_thinking_base,
        avg(f.baseline_vocabulary_base) as vocabulary_base,
        avg(f.baseline_grammar_base) as grammar_base
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, d.student_grade_base
),

RC_analysis_midline as (
    select
        d.city_mid as city,
        d.student_grade_mid as grade,
        avg(f.midline_factual_mid) as factual_mid,
        avg(f.midline_inference_mid) as inference_mid,
        avg(f.midline_critical_thinking_mid) as critical_thinking_mid,
        avg(f.midline_vocabulary_mid) as vocabulary_mid,
        avg(f.midline_grammar_mid) as grammar_mid
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as f
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as d
        on f.student_id = d.student_id
    where d.midline_attendence = True
    group by d.city_mid, d.student_grade_mid
),

ALL_COMBINATIONS as (
    select distinct
        city,
        grade
    from RC_analysis_baseline
    
    union
    
    select distinct
        city,
        grade
    from RC_analysis_midline
    
    -- union
    
    -- select distinct
    --     city,
    --     grade
    -- from RC_analysis_endline
)

select 
    ac.city,
    ac.grade,
    -- Factual scores
    b.factual_base,
    m.factual_mid,
    -- e.factual_end,
    -- Inference scores
    b.inference_base,
    m.inference_mid,
    -- e.inference_end,
    -- Critical Thinking scores
    b.critical_thinking_base,
    m.critical_thinking_mid,
    -- e.critical_thinking_end,
    -- Vocabulary scores
    b.vocabulary_base,
    m.vocabulary_mid,
    -- e.vocabulary_end,
    -- Grammar scores
    b.grammar_base,
    m.grammar_mid
    -- e.grammar_end
    
from ALL_COMBINATIONS as ac
left join RC_ANALYSIS_BASELINE as b
    on
        ac.CITY = b.CITY 
        and ac.GRADE = b.GRADE
left join RC_analysis_midline m
    on ac.city = m.city 
    and ac.grade = m.grade
-- left join RC_analysis_endline e
--     on ac.city = e.city 
--     and ac.grade = e.grade
order by ac.city, ac.grade
