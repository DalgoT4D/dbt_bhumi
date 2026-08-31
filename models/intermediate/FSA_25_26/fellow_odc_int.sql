{{ config(
  materialized='table',
  tags=["fsa_25_26", "int"]
) }}

WITH fellow_school AS (
    SELECT
        academic_year,
        fellow_id,
        fellow_name,
        pm_id,
        pm_name,
        cohort,
        funding_year,
        donor_id,
        donor_name,
        school_id,
        school_name,
        school_state,
        city,
        school_type,
        grade,
        grade_section
    FROM {{ ref('fellow_scl_cls_data') }}
),

odc AS (
    SELECT
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        cohort,
        year_map,
        school,
        grade,
        grade_section,
        COALESCE(COUNT(*), 0) AS odc_count,
        AVG(student_engagement_percentage) AS student_engagement_percentage
    FROM {{ ref('odc_fsa') }}
    GROUP BY
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        cohort,
        year_map,
        school,
        grade,
        grade_section
)

-- select * from odc

SELECT
    fs.academic_year,
    odc.month,
    odc.quarter,
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
    fs.grade,
    fs.grade_section,
    fs.school_type,
    odc.odc_count,
    odc.student_engagement_percentage
FROM fellow_school AS fs
LEFT JOIN odc AS odc 
    ON
        fs.academic_year = odc.academic_year
        AND fs.funding_year = odc.year_map
        AND fs.fellow_id = odc.fellow_id
        AND fs.school_name = odc.school
        AND fs.grade = odc.grade
        AND fs.grade_section = odc.grade_section
