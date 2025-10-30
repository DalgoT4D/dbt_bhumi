with RF_analysis_baseline as (
    select
        d.city_base as city,
        d.student_grade_base as grade,
        avg(f.baseline_letter_sounds_base) as letter_sounds_base,
        avg(f.baseline_cvc_words_base) as CVC_words_base,
        avg(f.baseline_blends_base) as blends_base,
        avg(f.baseline_consonant_diagraph_base) as consonant_diagraph_base,
        avg(f.baseline_magic_e_words_base) as magic_E_words_base,
        avg(f.baseline_vowel_diagraphs_base) as vowel_diagraphs_base,
        avg(f.baseline_multi_syllabelle_words_base) as multi_syllabelle_words_base,
        avg(f.baseline_passage_1_base) as passage_1_base,
        avg(f.baseline_passage_2_base) as passage_2_base
    from 
        {{ref('base_mid_end_comb_scores_25_26_fct')}} f
        inner join {{ref('base_mid_end_comb_students_25_26_dim')}} d
        on f.student_id = d.student_id
    where d.baseline_attendence = True
    group by d.city_base, d.student_grade_base
),

-- RF_analysis_midline as (
--     select
--         d.city_mid as city,
--         d.grade_taught_mid as grade,
--         avg(f.letter_sounds_mid) as letter_sounds_mid,
--         avg(f.CVC_words_mid) as CVC_words_mid,
--         avg(f.blends_mid) as blends_mid,
--         avg(f.consonant_diagraph_mid) as consonant_diagraph_mid,
--         avg(f.magic_E_words_mid) as magic_E_words_mid,
--         avg(f.vowel_diagraphs_mid) as vowel_diagraphs_mid,
--         avg(f.multi_syllabelle_words_mid) as multi_syllabelle_words_mid,
--         avg(f.passage_1_mid) as passage_1_mid,
--         avg(f.passage_2_mid) as passage_2_mid
--     from 
--         {{ref('base_mid_end_comb_scores_2425_fct')}} f
--         inner join {{ref('base_mid_end_comb_students_2425_dim')}} d
--         on f.student_id = d.student_id
--     where d.midline_attendence = True
--     group by d.city_mid, d.grade_taught_mid
-- ),

-- RF_analysis_endline as (
--     select
--         d.city_end as city,
--         d.grade_taught_end as grade,
--         avg(f.letter_sounds_end) as letter_sounds_end,
--         avg(f.CVC_words_end) as CVC_words_end,
--         avg(f.blends_end) as blends_end,
--         avg(f.consonant_diagraph_end) as consonant_diagraph_end,
--         avg(f.magic_E_words_end) as magic_E_words_end,
--         avg(f.vowel_diagraphs_end) as vowel_diagraphs_end,
--         avg(f.multi_syllabelle_words_end) as multi_syllabelle_words_end,
--         avg(f.passage_1_end) as passage_1_end,
--         avg(f.passage_2_end) as passage_2_end
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
    from RF_analysis_baseline
    
    -- union
    
    -- select distinct
    --     city,
    --     grade
    -- from RF_analysis_midline
    
    -- union
    
    -- select distinct
    --     city,
    --     grade
    -- from RF_analysis_endline
)

select 
    ac.city,
    ac.grade,
    
    --baseline
    b.letter_sounds_base,
    b.CVC_words_base,
    b.blends_base,
    b.consonant_diagraph_base,
    b.magic_E_words_base,
    b.vowel_diagraphs_base,
    b.multi_syllabelle_words_base,
    b.passage_1_base,
    b.passage_2_base

    -- --midline
    -- m.letter_sounds_mid,
    -- m.CVC_words_mid,
    -- m.blends_mid,
    -- m.consonant_diagraph_mid,
    -- m.magic_E_words_mid,
    -- m.vowel_diagraphs_mid,
    -- m.multi_syllabelle_words_mid,
    -- m.passage_1_mid,
    -- m.passage_2_mid,

    -- --endline
    -- e.letter_sounds_end,
    -- e.CVC_words_end,
    -- e.blends_end,
    -- e.consonant_diagraph_end,
    -- e.magic_E_words_end,
    -- e.vowel_diagraphs_end,
    -- e.multi_syllabelle_words_end,
    -- e.passage_1_end,
    -- e.passage_2_end

from all_combinations ac
left join RF_analysis_baseline b
    on ac.city = b.city 
    and ac.grade = b.grade
-- left join RF_analysis_midline m
--     on ac.city = m.city 
--     and ac.grade = m.grade
-- left join RF_analysis_endline e
--     on ac.city = e.city 
--     and ac.grade = e.grade
-- order by ac.city, ac.grade