with RF_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.STUDENT_GRADE_BASE as GRADE,
        D.DONOR_BASE as DONOR,
        D.PM_NAME_BASE as PM_NAME,
        D.FELLOW_NAME_BASE as FELLOW_NAME,
        F.RF_LEVEL_BASELINE_BASE as RF_LEVEL,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_BASE,
        count(distinct case when D.COHORT_BASE = '2024' then F.STUDENT_ID end) as COHORT_2024_COUNT_BASE,
        count(distinct case when D.COHORT_BASE = '2025' then F.STUDENT_ID end) as COHORT_2025_COUNT_BASE
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, D.STUDENT_GRADE_BASE, F.RF_LEVEL_BASELINE_BASE, D.DONOR_BASE, D.PM_NAME_BASE, D.FELLOW_NAME_BASE
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
        count(distinct case when D.COHORT_MID = '2024' then F.STUDENT_ID end) as COHORT_2024_COUNT_MID,
        count(distinct case when D.COHORT_MID = '2025' then F.STUDENT_ID end) as COHORT_2025_COUNT_MID
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.MIDLINE_ATTENDENCE = True
    group by D.CITY_MID, D.STUDENT_GRADE_MID, F.RF_LEVEL_MIDLINE_MID, D.DONOR_MID, D.PM_NAME_MID, D.FELLOW_NAME_MID
),

-- RF_analysis_endline as (
--     select
--         d.city_end as city,
--         d.grade_taught_end as grade,
--         f.RF_LEVEL_end as RF_LEVEL,
--         count(distinct f.student_id) as student_count_end
--     from 
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
--         on f.student_id = d.student_id
--     where d.endline_attendence = True
--     group by d.city_end, d.grade_taught_end, f.RF_LEVEL_end
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
    --     grade,
    --     RF_LEVEL
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
    B.COHORT_2024_COUNT_BASE,
    B.COHORT_2025_COUNT_BASE,
    M.STUDENT_COUNT_MID,
    M.COHORT_2024_COUNT_MID,
    M.COHORT_2025_COUNT_MID
    -- e.student_count_end
from ALL_COMBINATIONS as AC
left join RF_ANALYSIS_BASELINE as B
    on
        AC.CITY = B.CITY 
        and AC.RF_LEVEL = B.RF_LEVEL 
        and AC.GRADE = B.GRADE
        and AC.DONOR = B.DONOR
        and AC.PM_NAME = B.PM_NAME
        and AC.FELLOW_NAME = B.FELLOW_NAME
left join RF_ANALYSIS_MIDLINE as M
    on
        AC.CITY = M.CITY 
        and AC.RF_LEVEL = M.RF_LEVEL 
        and AC.GRADE = M.GRADE
        and AC.DONOR = M.DONOR
        and AC.PM_NAME = M.PM_NAME
        and AC.FELLOW_NAME = M.FELLOW_NAME
-- left join RF_analysis_endline e
--     on ac.city = e.city 
--     and ac.RF_LEVEL = e.RF_LEVEL 
--     and ac.grade = e.grade
order by AC.CITY, AC.GRADE, AC.RF_LEVEL
