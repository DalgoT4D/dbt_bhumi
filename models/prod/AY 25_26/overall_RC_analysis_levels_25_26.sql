with RC_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.STUDENT_GRADE_BASE as GRADE,
        D.DONOR_BASE as DONOR,
        D.PM_NAME_BASE as PM_NAME,
        D.FELLOW_NAME_BASE as FELLOW_NAME,
        F.RC_LEVEL_BASELINE_BASE as RC_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_BASE
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, D.STUDENT_GRADE_BASE, D.DONOR_BASE, D.PM_NAME_BASE, D.FELLOW_NAME_BASE, F.RC_LEVEL_BASELINE_BASE
),

RC_ANALYSIS_MIDLINE as (
    select
        D.CITY_MID as CITY,
        D.STUDENT_GRADE_MID as GRADE,
        D.DONOR_MID as DONOR,
        D.PM_NAME_MID as PM_NAME,
        D.FELLOW_NAME_MID as FELLOW_NAME,
        F.RC_LEVEL_MIDLINE_MID as RC_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_MID
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.MIDLINE_ATTENDENCE = True
    group by D.CITY_MID, D.STUDENT_GRADE_MID, D.DONOR_MID, D.PM_NAME_MID, D.FELLOW_NAME_MID, F.RC_LEVEL_MIDLINE_MID
),

RC_ANALYSIS_ENDLINE as (
    select
        D.CITY_END as CITY,
        D.STUDENT_GRADE_END as GRADE,
        D.DONOR_END as DONOR,
        D.PM_NAME_END as PM_NAME,
        D.FELLOW_NAME_END as FELLOW_NAME,
        F.RC_LEVEL_ENDLINE_END as RC_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_END
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.ENDLINE_ATTENDENCE = True
    group by D.CITY_END, D.STUDENT_GRADE_END, D.DONOR_END, D.PM_NAME_END, D.FELLOW_NAME_END, F.RC_LEVEL_ENDLINE_END
),

ALL_COMBINATIONS as (
    select distinct
        CITY,
        GRADE,
        DONOR,
        PM_NAME,
        FELLOW_NAME,
        RC_LEVEL
    from RC_ANALYSIS_BASELINE
    
    union
    
    select distinct
        CITY,
        GRADE,
        DONOR,
        PM_NAME,
        FELLOW_NAME,
        RC_LEVEL
    from RC_ANALYSIS_MIDLINE
    
    union
    
    select distinct
        CITY,
        GRADE,
        DONOR,
        PM_NAME,
        FELLOW_NAME,
        RC_LEVEL
    from RC_ANALYSIS_ENDLINE

)

select 
    AC.CITY,
    AC.GRADE,
    AC.DONOR,
    AC.PM_NAME,
    AC.FELLOW_NAME,
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
        and AC.FELLOW_NAME = B.FELLOW_NAME
        and AC.DONOR = B.DONOR
        and AC.PM_NAME = B.PM_NAME
left join RC_ANALYSIS_MIDLINE as M
    on
        AC.CITY = M.CITY 
        and AC.RC_LEVEL = M.RC_LEVEL 
        and AC.GRADE = M.GRADE
        and AC.FELLOW_NAME = M.FELLOW_NAME
        and AC.DONOR = M.DONOR
        and AC.PM_NAME = M.PM_NAME
left join RC_ANALYSIS_ENDLINE as E
    on
        AC.CITY = E.CITY 
        and AC.RC_LEVEL = E.RC_LEVEL 
        and AC.GRADE = E.GRADE
        and AC.FELLOW_NAME = E.FELLOW_NAME
        and AC.DONOR = E.DONOR
        and AC.PM_NAME = E.PM_NAME
order by AC.CITY, AC.GRADE, AC.RC_LEVEL
