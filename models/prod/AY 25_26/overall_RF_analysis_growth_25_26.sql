with RF_ANALYSIS_BASELINE as (
    select
        D.CITY_BASE as CITY,
        D.STUDENT_GRADE_BASE as GRADE,
        F.RF_BASELINE_GROWTH_BASE as RF_GROWTH,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_BASE,
        count(distinct case when D.COHORT_BASE = '2024' then F.STUDENT_ID end) as COHORT_2024_COUNT_BASE,
        count(distinct case when D.COHORT_BASE = '2025' then F.STUDENT_ID end) as COHORT_2025_COUNT_BASE
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.BASELINE_ATTENDENCE = True
    group by D.CITY_BASE, D.STUDENT_GRADE_BASE, F.RF_BASELINE_GROWTH_BASE
),

RF_ANALYSIS_MIDLINE as (
    select
        D.CITY_MID as CITY,
        D.STUDENT_GRADE_MID as GRADE,
        F.RF_MIDLINE_GROWTH_MID as RF_GROWTH,
        count(distinct F.STUDENT_ID) as STUDENT_COUNT_MID,
        count(distinct case when D.COHORT_MID = '2024' then F.STUDENT_ID end) as COHORT_2024_COUNT_MID,
        count(distinct case when D.COHORT_MID = '2025' then F.STUDENT_ID end) as COHORT_2025_COUNT_MID
    from 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
    inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
        on F.STUDENT_ID = D.STUDENT_ID
    where D.MIDLINE_ATTENDENCE = True
    group by D.CITY_MID, D.STUDENT_GRADE_MID, F.RF_MIDLINE_GROWTH_MID
),

-- RF_analysis_endline as (
--     select
--         d.city_end as city,
--         d.grade_taught_end as grade,
--         f.RF_status_end as RF_status,
--         count(distinct f.student_id) as student_count_end
--     from 
--         {{ ref('base_mid_end_comb_scores_2425_fct') }} f
--         inner join {{ ref('base_mid_end_comb_students_2425_dim') }} d
--         on f.student_id = d.student_id
--     where d.endline_attendence = True
--     group by d.city_end, d.grade_taught_end, f.RF_status_end
-- ),

ALL_COMBINATIONS as (
    select distinct
        CITY,
        GRADE,
        RF_GROWTH
    from RF_ANALYSIS_BASELINE
    
    union
    
    select distinct
        CITY,
        GRADE,
        RF_GROWTH
    from RF_ANALYSIS_MIDLINE
    
    -- union
    
    -- select distinct
    --     city,
    --     grade,
    --     RF_status
    -- from RF_analysis_endline
)

select 
    AC.CITY,
    AC.GRADE,
    AC.RF_GROWTH,
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
        and AC.RF_GROWTH = B.RF_GROWTH 
        and AC.GRADE = B.GRADE
left join RF_ANALYSIS_MIDLINE as M
    on
        AC.CITY = M.CITY 
        and AC.RF_GROWTH = M.RF_GROWTH 
        and AC.GRADE = M.GRADE
-- left join RF_analysis_endline e
--     on ac.city = e.city 
--     and ac.RF_status = e.RF_status 
--     and ac.grade = e.grade
order by AC.CITY, AC.GRADE, AC.RF_GROWTH
