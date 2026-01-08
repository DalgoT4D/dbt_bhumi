with RC_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.GRADE_TAUGHT_BASE as GRADE,
        F.RC_LEVEL_BASE as RC_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_BASE
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, D.GRADE_TAUGHT_BASE, F.RC_LEVEL_BASE
),

RC_ANALYSIS_MIDLINE as (
    select
        D.CITY_MID as CITY,
        D.GRADE_TAUGHT_MID as GRADE,
        F.RC_LEVEL_MID as RC_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_MID
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.MIDLINE_ATTENDENCE = True
    group by D.CITY_MID, D.GRADE_TAUGHT_MID, F.RC_LEVEL_MID
),

RC_ANALYSIS_ENDLINE as (
    select
        D.CITY_END as CITY,
        D.GRADE_TAUGHT_END as GRADE,
        F.RC_LEVEL_END as RC_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_END
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.ENDLINE_ATTENDENCE = True
    group by D.CITY_END, D.GRADE_TAUGHT_END, F.RC_LEVEL_END
),

ALL_COMBINATIONS as (
    select distinct
        CITY,
        GRADE,
        RC_LEVEL
    from RC_ANALYSIS_BASELINE
    
    union
    
    select distinct
        CITY,
        GRADE,
        RC_LEVEL
    from RC_ANALYSIS_MIDLINE
    
    union
    
    select distinct
        CITY,
        GRADE,
        RC_LEVEL
    from RC_ANALYSIS_ENDLINE
)

select 
    AC.CITY,
    AC.GRADE,
    AC.RC_LEVEL,
    B.STUDENT_COUNT_BASE,
    M.STUDENT_COUNT_MID,
    E.STUDENT_COUNT_END
from ALL_COMBINATIONS as AC
left join RC_ANALYSIS_BASELINE as B
    on
        AC.CITY = B.CITY 
        and AC.RC_LEVEL = B.RC_LEVEL 
        and AC.GRADE = B.GRADE
left join RC_ANALYSIS_MIDLINE as M
    on
        AC.CITY = M.CITY 
        and AC.RC_LEVEL = M.RC_LEVEL 
        and AC.GRADE = M.GRADE
left join RC_ANALYSIS_ENDLINE as E
    on
        AC.CITY = E.CITY 
        and AC.RC_LEVEL = E.RC_LEVEL 
        and AC.GRADE = E.GRADE
order by AC.CITY, AC.GRADE, AC.RC_LEVEL
