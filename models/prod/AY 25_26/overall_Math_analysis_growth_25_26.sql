-- with RF_ANALYSIS_BASELINE as (
--     select
--         D.CITY_BASE as CITY,
--         D.STUDENT_GRADE_BASE as GRADE,
--         D.DONOR_BASE as DONOR,
--         D.PM_NAME_BASE as PM_NAME,
--         F.RF_BASELINE_GROWTH_BASE as RF_GROWTH,
--         count(distinct F.STUDENT_ID) as STUDENT_COUNT_BASE,
--         count(distinct case when D.COHORT_BASE = '2024' then F.STUDENT_ID end) as COHORT_2024_COUNT_BASE,
--         count(distinct case when D.COHORT_BASE = '2025' then F.STUDENT_ID end) as COHORT_2025_COUNT_BASE
--     from 
--         {{ ref('base_mid_end_comb_scores_25_26_fct') }} as F
--     inner join {{ ref('base_mid_end_comb_students_25_26_dim') }} as D
--         on F.STUDENT_ID = D.STUDENT_ID
--     where D.BASELINE_ATTENDENCE = True
--     group by D.CITY_BASE, D.STUDENT_GRADE_BASE, F.RF_BASELINE_GROWTH_BASE, D.DONOR_BASE, D.PM_NAME_BASE
-- ),

WITH RC_ANALYSIS_MIDLINE AS (
    SELECT
        D.CITY_MID AS CITY,
        D.STUDENT_GRADE_MID AS GRADE,
        D.DONOR_MID AS DONOR,
        D.PM_NAME_MID AS PM_NAME,
        F.MATH_MIDLINE_GROWTH_STATUS_MID AS MATH_GROWTH,
        count(DISTINCT F.STUDENT_ID) AS STUDENT_COUNT_MID,
        count(DISTINCT CASE WHEN D.COHORT_MID = '2024' THEN F.STUDENT_ID END) AS COHORT_2024_COUNT_MID,
        count(DISTINCT CASE WHEN D.COHORT_MID = '2025' THEN F.STUDENT_ID END) AS COHORT_2025_COUNT_MID
    FROM 
        {{ ref('base_mid_end_comb_scores_25_26_fct') }} AS F
    INNER JOIN {{ ref('base_mid_end_comb_students_25_26_dim') }} AS D
        ON F.STUDENT_ID = D.STUDENT_ID
    WHERE D.MIDLINE_ATTENDENCE = True
    GROUP BY D.CITY_MID, D.STUDENT_GRADE_MID, F.MATH_MIDLINE_GROWTH_STATUS_MID, D.DONOR_MID, D.PM_NAME_MID
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

ALL_COMBINATIONS AS (
    -- select distinct
    --     CITY,
    --     GRADE,
    --     DONOR,
    --     PM_NAME,
    --     RF_GROWTH
    -- from RF_ANALYSIS_BASELINE
    
    -- union
    
    SELECT DISTINCT
        CITY,
        GRADE,
        DONOR,
        PM_NAME,
        MATH_GROWTH
    FROM RC_ANALYSIS_MIDLINE
    
    -- union
    
    -- select distinct
    --     city,
    --     grade,
    --     RF_status
    -- from RF_analysis_endline
)

SELECT 
    AC.CITY,
    AC.GRADE,
    AC.DONOR,
    AC.PM_NAME,
    AC.MATH_GROWTH,
    M.STUDENT_COUNT_MID,
    M.COHORT_2024_COUNT_MID,
    M.COHORT_2025_COUNT_MID
-- B.STUDENT_COUNT_BASE,
-- B.COHORT_2024_COUNT_BASE,
-- B.COHORT_2025_COUNT_BASE,
-- e.student_count_end
FROM ALL_COMBINATIONS AS AC
-- left join RC_ANALYSIS_BASELINE as B
--     on
--         AC.CITY = B.CITY 
--         and AC.RC_GROWTH = B.RC_GROWTH 
--         and AC.GRADE = B.GRADE
--         and AC.DONOR = B.DONOR
--         and AC.PM_NAME = B.PM_NAME
LEFT JOIN RC_ANALYSIS_MIDLINE AS M
    ON
        AC.CITY = M.CITY 
        AND AC.MATH_GROWTH = M.MATH_GROWTH 
        AND AC.GRADE = M.GRADE
        AND AC.DONOR = M.DONOR
        AND AC.PM_NAME = M.PM_NAME
-- left join RF_analysis_endline e
--     on ac.city = e.city 
--     and ac.RC_status = e.RC_status 
--     and ac.grade = e.grade
ORDER BY AC.CITY, AC.GRADE, AC.MATH_GROWTH
