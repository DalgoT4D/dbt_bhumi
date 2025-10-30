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
        {{ref('base_mid_end_comb_scores_25_26_fct')}} f
        inner join {{ref('base_mid_end_comb_students_25_26_dim')}} d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, d.student_grade_base
),

-- RC_analysis_midline as (
--     select
--         d.city_mid as city,
--         d.grade_taught_mid as grade,
--         avg(f.factual_mid) as factual_mid,
--         avg(f.inference_mid) as inference_mid,
--         avg(f.critical_thinking_mid) as critical_thinking_mid,
--         avg(f.vocabulary_mid) as vocabulary_mid,
--         avg(f.grammar_mid) as grammar_mid
--     from 
--         {{ref('base_mid_end_comb_scores_2425_fct')}} f
--         inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
--         on f.student_id = d.student_id
--     where d.midline_attendence = True
--     group by d.city_mid, d.grade_taught_mid
-- ),

-- RC_analysis_endline as (
--     select
--         d.city_end as city,
--         d.grade_taught_end as grade,
--         avg(f.factual_end) as factual_end,
--         avg(f.inference_end) as inference_end,
--         avg(f.critical_thinking_end) as critical_thinking_end,
--         avg(f.vocabulary_end) as vocabulary_end,
--         avg(f.grammar_end) as grammar_end
--     from 
--         {{ref('base_mid_end_comb_scores_2425_fct')}} f
--         inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
--         on f.student_id = d.student_id
--     where d.endline_attendence = True
--     group by d.city_end, d.grade_taught_end
-- ),

all_combinations as (
    select distinct
        city,
        grade
    from RC_analysis_baseline
    
    -- union
    
    -- select distinct
    --     city,
    --     grade
    -- from RC_analysis_midline
    
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
    -- m.factual_mid,
    -- e.factual_end,
    -- Inference scores
    b.inference_base,
    -- m.inference_mid,
    -- e.inference_end,
    -- Critical Thinking scores
    b.critical_thinking_base,
    -- m.critical_thinking_mid,
    -- e.critical_thinking_end,
    -- Vocabulary scores
    b.vocabulary_base,
    -- m.vocabulary_mid,
    -- e.vocabulary_end,
    -- Grammar scores
    b.grammar_base
    -- m.grammar_mid,
    -- e.grammar_end
    
from all_combinations ac
left join RC_analysis_baseline b
    on ac.city = b.city 
    and ac.grade = b.grade
-- left join RC_analysis_midline m
--     on ac.city = m.city 
--     and ac.grade = m.grade
-- left join RC_analysis_endline e
--     on ac.city = e.city 
--     and ac.grade = e.grade
-- order by ac.city, ac.grade