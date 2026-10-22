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

odc AS (
    SELECT
        academic_year,
        month,
        quarter,
        fellow_id,
        -- fellow_name,
        -- pm_name,
        -- funding_year,
        -- cohort,
        -- school_name,
        -- grade,
        -- grade_section,
        sum(odc_count) AS odc_count
    FROM {{ ref('fellow_odc_int') }}
    GROUP BY
        academic_year,
        month,
        quarter,
        fellow_id
        -- fellow_name,
        -- pm_name,
        -- funding_year,
        -- cohort,
        -- school_name,
        -- grade,
        -- grade_section
),

checkins AS (
    SELECT
        academic_year,
        month,
        quarter,
        fellow_id,
        -- fellow_name,
        -- pm_name,
        -- funding_year,
        -- cohort,
        -- school_name,
        -- grade,
        -- grade_section,
        sum(checkins_count) AS checkins_count
    FROM {{ ref('fellow_checkins') }}
    GROUP BY
        academic_year,
        month,
        quarter,
        fellow_id
        -- fellow_name,
        -- pm_name,
        -- funding_year,
        -- cohort,
        -- school_name,
        -- grade,
        -- grade_section
),

fcm_updates AS (
    SELECT
        academic_year,
        month,
        quarter,
        fellow_id,
        -- fellow_name,
        -- pm_name,
        -- funding_year,
        -- cohort,
        -- school_name,
        -- grade,
        -- grade_section,
        sum(fcm_update_count) AS fcm_update_count
    FROM {{ ref('fellow_fcm_int') }}
    GROUP BY
        academic_year,
        month,
        quarter,
        fellow_id
        -- fellow_name,
        -- pm_name,
        -- funding_year,
        -- cohort,
        -- school_name,
        -- grade,
        -- grade_section
),

classroom_updates AS (
    SELECT
        academic_year,
        month,
        quarter,
        fellow_id,
        -- fellow_name,
        -- pm_name,
        -- funding_year,
        -- cohort,
        -- school_name,
        -- grade,
        -- grade_section,
        sum(school_leader_checkins) AS school_leader_checkins_count,
        sum(teacher_circles) AS teacher_circle_count,
        sum(homes_visited) AS community_visit_count,
        sum(teaching_hours) AS teaching_hours_sum,
        sum(helo_circles) AS helo_circles_count,
        sum(ptms) AS ptms_count
    FROM {{ ref('class_upd_int') }}
    GROUP BY
        academic_year,
        month,
        quarter,
        fellow_id
        -- fellow_name,
        -- pm_name,
        -- funding_year,
        -- cohort,
        -- school_name,
        -- grade,
        -- grade_section
)

SELECT
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
    o.odc_count,
    c.checkins_count,
    f.fcm_update_count,
    cu.school_leader_checkins_count,
    cu.teacher_circle_count,
    cu.community_visit_count,
    cu.teaching_hours_sum,
    cu.helo_circles_count,
    cu.ptms_count
FROM fellow_school AS fs
LEFT JOIN odc AS o
    ON
        fs.academic_year = o.academic_year
        AND fs.month = o.month
        AND fs.quarter = o.quarter
        AND fs.fellow_id = o.fellow_id
        -- AND fs.school_name = o.school_name
        -- AND fs.grade = o.grade
        -- AND fs.grade_section = o.grade_section
LEFT JOIN checkins AS c
    ON
        fs.academic_year = c.academic_year
        AND fs.month = c.month
        AND fs.quarter = c.quarter
        AND fs.fellow_id = c.fellow_id
        -- AND fs.school_name = c.school_name
        -- AND fs.grade = c.grade
        -- AND fs.grade_section = c.grade_section
LEFT JOIN fcm_updates AS f
    ON
        fs.academic_year = f.academic_year
        AND fs.month = f.month
        AND fs.quarter = f.quarter
        AND fs.fellow_id = f.fellow_id
        -- AND fs.school_name = f.school_name
        -- AND fs.grade = f.grade
        -- AND fs.grade_section = f.grade_section
LEFT JOIN classroom_updates AS cu
    ON
        fs.academic_year = cu.academic_year
        AND fs.month = cu.month
        AND fs.quarter = cu.quarter
        AND fs.fellow_id = cu.fellow_id
        -- AND fs.school_name = cu.school_name
        -- AND fs.grade = cu.grade
        -- AND fs.grade_section = cu.grade_section
