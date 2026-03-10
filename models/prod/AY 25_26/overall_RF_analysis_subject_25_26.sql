with RF_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.STUDENT_GRADE_BASE as GRADE,
        D.DONOR_BASE as DONOR,
        D.PM_NAME_BASE as PM_NAME,
        D.FELLOW_NAME_BASE as FELLOW_NAME,
        F.RF_LEVEL_BASELINE_BASE as RF_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_BASE,
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
    group by D.CITY_BASE, D.STUDENT_GRADE_BASE, F.RF_LEVEL_BASELINE_BASE, D.FELLOW_NAME_BASE, D.DONOR_BASE, D.PM_NAME_BASE  
),

RF_ANALYSIS_MIDLINE as (
    select
        D.CITY_MID as CITY,
        D.STUDENT_GRADE_MID as GRADE,
        D.DONOR_MID as DONOR,
        D.PM_NAME_MID as PM_NAME,
        D.FELLOW_NAME_MID as FELLOW_NAME,
        F.RF_LEVEL_MIDLINE_MID as RF_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_MID,
        avg(F.MIDLINE_LETTER_SOUNDS_MID) as LETTER_SOUNDS_MID,
        avg(F.MIDLINE_CVC_WORDS_MID) as CVC_WORDS_MID,
        avg(F.MIDLINE_BLENDS_MID) as BLENDS_MID,
        avg(F.MIDLINE_CONSONANT_DIAGRAPH_MID) as CONSONANT_DIAGRAPH_MID,
        avg(F.MIDLINE_MAGIC_E_WORDS_MID) as MAGIC_E_WORDS_MID,
        avg(F.MIDLINE_VOWEL_DIAGRAPHS_MID) as VOWEL_DIAGRAPHS_MID,
        avg(F.MIDLINE_MULTI_SYLLABELLE_WORDS_MID) as MULTI_SYLLABELLE_WORDS_MID,
        avg(F.MIDLINE_PASSAGE_1_MID) as PASSAGE_1_MID,
        avg(F.MIDLINE_PASSAGE_2_MID) as PASSAGE_2_MID
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.MIDLINE_ATTENDENCE = True
    group by D.CITY_MID, D.STUDENT_GRADE_MID, F.RF_LEVEL_MIDLINE_MID, D.FELLOW_NAME_MID, D.DONOR_MID, D.PM_NAME_MID
),

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
        GRADE,
        DONOR,
        PM_NAME,
        FELLOW_NAME,
        RF_LEVEL
    from RF_ANALYSIS_BASELINE
    
    union
    
    select distinct
        CITY,
        GRADE,
        DONOR,
        PM_NAME,
        FELLOW_NAME,
        RF_LEVEL
    from RF_ANALYSIS_MIDLINE
    
    -- union
    
    -- select distinct
    --     city,
    --     grade
    -- from RF_analysis_endline
)

select 
    AC.CITY,
    AC.GRADE,
    AC.DONOR,
    AC.PM_NAME,
    AC.FELLOW_NAME,
    AC.RF_LEVEL,
    
    B.STUDENT_COUNT_BASE,
    M.STUDENT_COUNT_MID,
    --baseline
    B.LETTER_SOUNDS_BASE,
    B.CVC_WORDS_BASE,
    B.BLENDS_BASE,
    B.CONSONANT_DIAGRAPH_BASE,
    B.MAGIC_E_WORDS_BASE,
    B.VOWEL_DIAGRAPHS_BASE,
    B.MULTI_SYLLABELLE_WORDS_BASE,
    B.PASSAGE_1_BASE,
    B.PASSAGE_2_BASE,

    --midline
    M.LETTER_SOUNDS_MID,
    M.CVC_WORDS_MID,
    M.BLENDS_MID,
    M.CONSONANT_DIAGRAPH_MID,
    M.MAGIC_E_WORDS_MID,
    M.VOWEL_DIAGRAPHS_MID,
    M.MULTI_SYLLABELLE_WORDS_MID,
    M.PASSAGE_1_MID,
    M.PASSAGE_2_MID

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
        and AC.RF_LEVEL = B.RF_LEVEL
        and AC.FELLOW_NAME = B.FELLOW_NAME
        and AC.DONOR = B.DONOR
        and AC.PM_NAME = B.PM_NAME
left join RF_ANALYSIS_MIDLINE as M
    on
        AC.CITY = M.CITY 
        and AC.GRADE = M.GRADE
        and AC.RF_LEVEL = M.RF_LEVEL
        and AC.FELLOW_NAME = M.FELLOW_NAME
        and AC.DONOR = M.DONOR
        and AC.PM_NAME = M.PM_NAME
-- left join RF_analysis_endline e
--     on ac.city = e.city 
--     and ac.grade = e.grade
order by AC.CITY, AC.GRADE
