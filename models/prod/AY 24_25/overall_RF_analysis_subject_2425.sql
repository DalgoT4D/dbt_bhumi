with RF_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.GRADE_TAUGHT_BASE as GRADE,
        avg(F.LETTER_SOUNDS_BASE) as LETTER_SOUNDS_BASE,
        avg(F.CVC_WORDS_BASE) as CVC_WORDS_BASE,
        avg(F.BLENDS_BASE) as BLENDS_BASE,
        avg(F.CONSONANT_DIAGRAPH_BASE) as CONSONANT_DIAGRAPH_BASE,
        avg(F.MAGIC_E_WORDS_BASE) as MAGIC_E_WORDS_BASE,
        avg(F.VOWEL_DIAGRAPHS_BASE) as VOWEL_DIAGRAPHS_BASE,
        avg(F.MULTI_SYLLABELLE_WORDS_BASE) as MULTI_SYLLABELLE_WORDS_BASE,
        avg(F.PASSAGE_1_BASE) as PASSAGE_1_BASE,
        avg(F.PASSAGE_2_BASE) as PASSAGE_2_BASE
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, D.GRADE_TAUGHT_BASE
),

RF_ANALYSIS_MIDLINE as (
    select
        D.CITY_MID as CITY,
        D.GRADE_TAUGHT_MID as GRADE,
        avg(F.LETTER_SOUNDS_MID) as LETTER_SOUNDS_MID,
        avg(F.CVC_WORDS_MID) as CVC_WORDS_MID,
        avg(F.BLENDS_MID) as BLENDS_MID,
        avg(F.CONSONANT_DIAGRAPH_MID) as CONSONANT_DIAGRAPH_MID,
        avg(F.MAGIC_E_WORDS_MID) as MAGIC_E_WORDS_MID,
        avg(F.VOWEL_DIAGRAPHS_MID) as VOWEL_DIAGRAPHS_MID,
        avg(F.MULTI_SYLLABELLE_WORDS_MID) as MULTI_SYLLABELLE_WORDS_MID,
        avg(F.PASSAGE_1_MID) as PASSAGE_1_MID,
        avg(F.PASSAGE_2_MID) as PASSAGE_2_MID
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.MIDLINE_ATTENDENCE = True
    group by D.CITY_MID, D.GRADE_TAUGHT_MID
),

RF_ANALYSIS_ENDLINE as (
    select
        D.CITY_END as CITY,
        D.GRADE_TAUGHT_END as GRADE,
        avg(F.LETTER_SOUNDS_END) as LETTER_SOUNDS_END,
        avg(F.CVC_WORDS_END) as CVC_WORDS_END,
        avg(F.BLENDS_END) as BLENDS_END,
        avg(F.CONSONANT_DIAGRAPH_END) as CONSONANT_DIAGRAPH_END,
        avg(F.MAGIC_E_WORDS_END) as MAGIC_E_WORDS_END,
        avg(F.VOWEL_DIAGRAPHS_END) as VOWEL_DIAGRAPHS_END,
        avg(F.MULTI_SYLLABELLE_WORDS_END) as MULTI_SYLLABELLE_WORDS_END,
        avg(F.PASSAGE_1_END) as PASSAGE_1_END,
        avg(F.PASSAGE_2_END) as PASSAGE_2_END
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
    from RF_ANALYSIS_BASELINE
    
    union
    
    select distinct
        CITY,
        GRADE
    from RF_ANALYSIS_MIDLINE
    
    union
    
    select distinct
        CITY,
        GRADE
    from RF_ANALYSIS_ENDLINE
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
    M.PASSAGE_2_MID,

    --endline
    E.LETTER_SOUNDS_END,
    E.CVC_WORDS_END,
    E.BLENDS_END,
    E.CONSONANT_DIAGRAPH_END,
    E.MAGIC_E_WORDS_END,
    E.VOWEL_DIAGRAPHS_END,
    E.MULTI_SYLLABELLE_WORDS_END,
    E.PASSAGE_1_END,
    E.PASSAGE_2_END

from ALL_COMBINATIONS as AC
left join RF_ANALYSIS_BASELINE as B
    on
        AC.CITY = B.CITY 
        and AC.GRADE = B.GRADE
left join RF_ANALYSIS_MIDLINE as M
    on
        AC.CITY = M.CITY 
        and AC.GRADE = M.GRADE
left join RF_ANALYSIS_ENDLINE as E
    on
        AC.CITY = E.CITY 
        and AC.GRADE = E.GRADE
order by AC.CITY, AC.GRADE
