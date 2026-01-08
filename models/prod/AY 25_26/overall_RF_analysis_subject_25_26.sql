with RF_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.STUDENT_GRADE_BASE as GRADE,
        avg(F.BASELINE_LETTER_SOUNDS_BASE) as LETTER_SOUNDS_BASE,
        avg(F.BASELINE_CVC_WORDS_BASE) as CVC_WORDS_BASE,
        avg(F.BASELINE_BLENDS_BASE) as BLENDS_BASE,
        avg(F.BASELINE_CONSONANT_DIAGRAPH_BASE) as CONSONANT_DIAGRAPH_BASE,
        avg(F.BASELINE_MAGIC_E_WORDS_BASE) as MAGIC_E_WORDS_BASE,
        avg(F.BASELINE_VOWEL_DIAGRAPHS_BASE) as VOWEL_DIAGRAPHS_BASE,
        avg(F.BASELINE_MULTI_SYLLABELLE_WORDS_BASE) as MULTI_SYLLABELLE_WORDS_BASE,
        avg(F.BASELINE_PASSAGE_1_BASE) as PASSAGE_1_BASE,
        avg(F.BASELINE_PASSAGE_2_BASE) as PASSAGE_2_BASE
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, D.STUDENT_GRADE_BASE
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
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
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
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
--         on f.student_id = d.student_id
--     where d.endline_attendence = True
--     group by d.city_end, d.grade_taught_end
-- ),

ALL_COMBINATIONS as (
    select distinct
        CITY,
        GRADE
    from RF_ANALYSIS_BASELINE
    
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
    AC.CITY,
    AC.GRADE,
    
    --baseline
    B.LETTER_SOUNDS_BASE,
    B.CVC_WORDS_BASE,
    B.BLENDS_BASE,
    B.CONSONANT_DIAGRAPH_BASE,
    B.MAGIC_E_WORDS_BASE,
    B.VOWEL_DIAGRAPHS_BASE,
    B.MULTI_SYLLABELLE_WORDS_BASE,
    B.PASSAGE_1_BASE,
    B.PASSAGE_2_BASE

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

from ALL_COMBINATIONS as AC
left join RF_ANALYSIS_BASELINE as B
    on
        AC.CITY = B.CITY 
        and AC.GRADE = B.GRADE
-- left join RF_analysis_midline m
--     on ac.city = m.city 
--     and ac.grade = m.grade
-- left join RF_analysis_endline e
--     on ac.city = e.city 
--     and ac.grade = e.grade
-- order by ac.city, ac.grade
