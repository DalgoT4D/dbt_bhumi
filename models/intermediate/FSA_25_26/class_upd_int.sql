{{ config(
  materialized='table',
  tags=["fsa_25_26", "int"]
) }}

WITH fellow_school AS (
    SELECT
        academic_year,
        quarter,
        month,
        fellow_id,
        fellow_name,
        cohort,
        funding_year, 
        donor_name,
        pm_name,
        school_name,
        school_state,
        city,
        school_type,
        grade, 
        grade_section
    FROM {{ ref('fy_fellow_sch_cls_int') }}
),

classroom_updates AS (
    SELECT
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        pm_name,
        year_map,
        cohort,
        school_name,
        grade,
        grade_section,

        SUM(ptms) AS ptms,
        SUM(helo_circles) AS helo_circles,
        ROUND(AVG(homes_visited), 2) AS homes_visited,
        SUM(teaching_hours) AS teaching_hours,
        SUM(teacher_circles) AS teacher_circles,
        SUM(school_leader_checkins) AS school_leader_checkins
        -- SUM(mathematics_score) AS mathematics_score,
        -- SUM(reading_comprehension_score) AS reading_comprehension_score
    FROM {{ ref('classroom_updates') }}
    GROUP BY
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        pm_name,
        year_map,
        cohort,
        school_name,
        grade,
        grade_section
)

SELECT DISTINCT
    fs.academic_year,
    fs.month,
    fs.quarter,
    fs.fellow_id,
    fs.fellow_name,
    fs.pm_name,
    fs.cohort,
    fs.funding_year,
    fs.donor_name,
    fs.school_name,
    fs.school_state,
    fs.city,
    fs.school_type,
    fs.grade,
    fs.grade_section,
    cu.ptms,
    cu.helo_circles,
    cu.homes_visited,
    cu.teaching_hours,
    cu.teacher_circles,
    cu.school_leader_checkins
    -- cu.mathematics_score,
    -- cu.reading_comprehension_score
FROM fellow_school AS fs
LEFT JOIN classroom_updates AS cu
    ON
        fs.academic_year = cu.academic_year
        AND fs.fellow_id = cu.fellow_id
        -- AND fs.school_name = cu.school_name
        -- AND fs.grade = cu.grade
        -- AND fs.grade_section = cu.grade_section
        AND fs.month = cu.month
        AND fs.quarter = cu.quarter
