{{ config(
  materialized='table',
  tags=["fsa_25_26", "int"]
) }}

WITH fellow_school AS (
    SELECT
        fellow_id,
        fellow_name,
        pm_id,
        pm_name,
        cohort,
        funding_year,
        academic_year,
        school_name,
        grade,
        grade_section,
        school_state,
        city,
        school_type,
        donor_name
    FROM {{ ref('fellow_scl_cls_data') }}
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
        AVG(homes_visited) AS homes_visited,
        SUM(teaching_hours) AS teaching_hours,
        SUM(teacher_circles) AS teacher_circles,
        -- SUM(mathematics_score) AS mathematics_score,
        SUM(school_leader_checkins) AS school_leader_checkins
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
    cu.month,
    cu.quarter,
    fs.fellow_id,
    fs.fellow_name,
    fs.pm_id,
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
    -- cu.mathematics_score,
    cu.school_leader_checkins
    -- cu.reading_comprehension_score
FROM fellow_school AS fs
LEFT JOIN classroom_updates AS cu
    ON
        fs.academic_year = cu.academic_year
        AND fs.funding_year = cu.year_map
        AND fs.fellow_id = cu.fellow_id
        AND fs.school_name = cu.school_name
        AND fs.grade = cu.grade
        AND fs.grade_section = cu.grade_section
