with RF_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.STUDENT_GRADE_BASE as GRADE,
        D.DONOR_BASE as DONOR,
        D.PM_NAME_BASE as PM_NAME,
        D.FELLOW_NAME_BASE as FELLOW_NAME,
        F.RF_BASELINE_GROWTH_BASE as RF_GROWTH,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_BASE,
        count(distinct case when D.COHORT_BASE = '2024' then F.STUDENT_ID end) as COHORT_2024_COUNT_BASE,
        count(distinct case when D.COHORT_BASE = '2025' then F.STUDENT_ID end) as COHORT_2025_COUNT_BASE
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, D.STUDENT_GRADE_BASE, F.RF_BASELINE_GROWTH_BASE, D.DONOR_BASE, D.PM_NAME_BASE, D.FELLOW_NAME_BASE
),

RF_ANALYSIS_MIDLINE as (
    select
        D.CITY_MID as CITY,
        D.STUDENT_GRADE_MID as GRADE,
        D.DONOR_MID as DONOR,
        D.PM_NAME_MID as PM_NAME,
        D.FELLOW_NAME_MID as FELLOW_NAME,
        F.RF_MIDLINE_GROWTH_MID as RF_GROWTH,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_MID,
        count(distinct case when D.COHORT_MID = '2024' then F.STUDENT_ID end) as COHORT_2024_COUNT_MID,
        count(distinct case when D.COHORT_MID = '2025' then F.STUDENT_ID end) as COHORT_2025_COUNT_MID
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.MIDLINE_ATTENDENCE = True
    group by D.CITY_MID, D.STUDENT_GRADE_MID, F.RF_MIDLINE_GROWTH_MID, D.DONOR_MID, D.PM_NAME_MID, D.FELLOW_NAME_MID
),

RF_ANALYSIS_ENDLINE as (
    select
        D.CITY_END as CITY,
        D.STUDENT_GRADE_END as GRADE,
        D.DONOR_END as DONOR,
        D.PM_NAME_END as PM_NAME,
        D.FELLOW_NAME_END as FELLOW_NAME,
        F.RF_ENDLINE_GROWTH_END as RF_GROWTH,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_END,
        count(distinct case when D.COHORT_END = '2024' then F.STUDENT_ID end) as COHORT_2024_COUNT_END,
        count(distinct case when D.COHORT_END = '2025' then F.STUDENT_ID end) as COHORT_2025_COUNT_END
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.ENDLINE_ATTENDENCE = True
    group by D.CITY_END, D.STUDENT_GRADE_END, F.RF_ENDLINE_GROWTH_END, D.DONOR_END, D.PM_NAME_END, D.FELLOW_NAME_END
),

ALL_COMBINATIONS as (
    select distinct
        CITY,
        GRADE,
        DONOR,
        PM_NAME,
        FELLOW_NAME,
        RF_GROWTH
    from RF_ANALYSIS_BASELINE
    
    union
    
    select distinct
        CITY,
        GRADE,
        DONOR,
        PM_NAME,
        FELLOW_NAME,
        RF_GROWTH
    from RF_ANALYSIS_MIDLINE
    
    union
    
    select distinct
        CITY,
        GRADE,
        DONOR,
        PM_NAME,
        FELLOW_NAME,
        RF_GROWTH  
    from RF_ANALYSIS_ENDLINE
)

select 
    AC.CITY,
    AC.GRADE,
    AC.DONOR,
    AC.PM_NAME,
    AC.FELLOW_NAME,
    AC.RF_GROWTH,
    B.STUDENT_COUNT_BASE,
    B.COHORT_2024_COUNT_BASE,
    B.COHORT_2025_COUNT_BASE,
    M.STUDENT_COUNT_MID,
    M.COHORT_2024_COUNT_MID,
    M.COHORT_2025_COUNT_MID,
    E.STUDENT_COUNT_END,
    E.COHORT_2024_COUNT_END,
    E.COHORT_2025_COUNT_END
from ALL_COMBINATIONS as AC
left join RF_ANALYSIS_BASELINE as B
    on
        AC.CITY = B.CITY 
        and AC.RF_GROWTH = B.RF_GROWTH 
        and AC.GRADE = B.GRADE
        and AC.DONOR = B.DONOR
        and AC.PM_NAME = B.PM_NAME
        and AC.FELLOW_NAME = B.FELLOW_NAME
left join RF_ANALYSIS_MIDLINE as M
    on
        AC.CITY = M.CITY 
        and AC.RF_GROWTH = M.RF_GROWTH 
        and AC.GRADE = M.GRADE
        and AC.DONOR = M.DONOR
        and AC.PM_NAME = M.PM_NAME
        and AC.FELLOW_NAME = M.FELLOW_NAME
left join RF_ANALYSIS_ENDLINE as E
    on
        AC.CITY = E.CITY 
        and AC.RF_GROWTH = E.RF_GROWTH 
        and AC.GRADE = E.GRADE
        and AC.DONOR = E.DONOR
        and AC.PM_NAME = E.PM_NAME
        and AC.FELLOW_NAME = E.FELLOW_NAME
order by AC.CITY, AC.GRADE, AC.RF_GROWTH
