with RF_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.GRADE_TAUGHT_BASE as GRADE,
        F.RF_STATUS_BASE as RF_STATUS,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_BASE
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, D.GRADE_TAUGHT_BASE, F.RF_STATUS_BASE
),

RF_ANALYSIS_MIDLINE as (
    select
        D.CITY_MID as CITY,
        D.GRADE_TAUGHT_MID as GRADE,
        F.RF_STATUS_MID as RF_STATUS,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_MID
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.MIDLINE_ATTENDENCE = True
    group by D.CITY_MID, D.GRADE_TAUGHT_MID, F.RF_STATUS_MID
),

RF_ANALYSIS_ENDLINE as (
    select
        D.CITY_END as CITY,
        D.GRADE_TAUGHT_END as GRADE,
        F.RF_STATUS_END as RF_STATUS,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_END
    from 
        {{ ref('base_mid_end_comb_scores_2425_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_2425_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.ENDLINE_ATTENDENCE = True
    group by D.CITY_END, D.GRADE_TAUGHT_END, F.RF_STATUS_END
),

ALL_COMBINATIONS as (
    select distinct
        CITY,
        GRADE,
        RF_STATUS
    from RF_ANALYSIS_BASELINE
    
    union
    
    select distinct
        CITY,
        GRADE,
        RF_STATUS
    from RF_ANALYSIS_MIDLINE
    
    union
    
    select distinct
        CITY,
        GRADE,
        RF_STATUS
    from RF_ANALYSIS_ENDLINE
)

select 
    AC.CITY,
    AC.GRADE,
    AC.RF_STATUS,
    B.STUDENT_COUNT_BASE,
    M.STUDENT_COUNT_MID,
    E.STUDENT_COUNT_END
from ALL_COMBINATIONS as AC
left join RF_ANALYSIS_BASELINE as B
    on
        AC.CITY = B.CITY 
        and AC.RF_STATUS = B.RF_STATUS 
        and AC.GRADE = B.GRADE
left join RF_ANALYSIS_MIDLINE as M
    on
        AC.CITY = M.CITY 
        and AC.RF_STATUS = M.RF_STATUS 
        and AC.GRADE = M.GRADE
left join RF_ANALYSIS_ENDLINE as E
    on
        AC.CITY = E.CITY 
        and AC.RF_STATUS = E.RF_STATUS 
        and AC.GRADE = E.GRADE
order by AC.CITY, AC.GRADE, AC.RF_STATUS
