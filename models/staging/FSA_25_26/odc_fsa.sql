{{ config(
  materialized='table',
  tags=["fsa_25_26", "staging"]
) }}

WITH odc AS (
    SELECT
        COALESCE(BTRIM(id::TEXT), '') AS odc_id,
        -- Date conversion from DD-MM-YYYY to date
        CASE
            WHEN NULLIF(TRIM(date::TEXT), '') IS NULL THEN NULL
            WHEN EXTRACT(MONTH FROM TRIM(date::TEXT)::DATE) <= 3
                THEN
                    (EXTRACT(YEAR FROM TRIM(date::TEXT)::DATE)::INTEGER - 1)::TEXT
                    || ' - ' || EXTRACT(YEAR FROM TRIM(date::TEXT)::DATE)::INTEGER::TEXT
            ELSE
                EXTRACT(YEAR FROM TRIM(date::TEXT)::DATE)::INTEGER::TEXT
                || ' - ' || (EXTRACT(YEAR FROM TRIM(date::TEXT)::DATE)::INTEGER + 1)::TEXT
        END AS academic_year,
        CASE
            WHEN NULLIF(TRIM(date::TEXT), '') IS NULL THEN NULL
            WHEN EXTRACT(MONTH FROM TRIM(date::TEXT)::DATE) <= 3
                THEN
                    EXTRACT(YEAR FROM TRIM(date::TEXT)::DATE)::INTEGER - 1
            ELSE
                EXTRACT(YEAR FROM TRIM(date::TEXT)::DATE)::INTEGER
        END AS year_map,
        CASE
            WHEN NULLIF(TRIM(date::TEXT), '') IS NULL THEN NULL
            ELSE TO_CHAR(TRIM(date::TEXT)::DATE, 'Mon')
        END AS month,
        CASE
            WHEN NULLIF(TRIM(date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(date::TEXT)::DATE
        END AS date,
        COALESCE(BTRIM(fellow_id::TEXT), '') AS fellow_id,
        COALESCE(INITCAP(BTRIM(fellow_name::TEXT)), '') AS fellow_name,
        COALESCE(BTRIM(programme_manager_id::TEXT), '') AS pm_id,
        COALESCE(INITCAP(BTRIM(pm_name::TEXT)), '') AS pm_name,
        COALESCE(BTRIM(cohort::TEXT), '') AS cohort,
        COALESCE(INITCAP(BTRIM(school::TEXT)), '') AS school,
        REGEXP_REPLACE(BTRIM(grade_observed::TEXT), '[^0-9]', '', 'g') AS grade,
        LOWER(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(BTRIM(grade_section::TEXT), ''), '-', '', 'g'), '([0-9])([a-zA-Z])', '\1 \2', 'g')) AS grade_section,
        COALESCE(BTRIM(city::TEXT), '') AS city,
        COALESCE(BTRIM(reporting_period::TEXT), '') AS reporting_period,
        COALESCE(BTRIM(grade_observed::TEXT), '') AS grade_observed,
        COALESCE(BTRIM(subject::TEXT), '') AS subject,
        is_completed,
        CASE
            WHEN NULLIF(TRIM(follow_up_date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(follow_up_date::TEXT)::DATE
        END AS follow_up_date,
        COALESCE(BTRIM(odc_notes::TEXT), '') AS odc_notes,
        COALESCE(BTRIM(strengths::TEXT), '') AS strengths,
        COALESCE(BTRIM(search_fts::TEXT), '') AS search_fts,
        COALESCE(BTRIM(action_plan::TEXT), '') AS action_plan,
        COALESCE(BTRIM(lesson_topic::TEXT), '') AS lesson_topic,
        CASE WHEN BTRIM(total_students::TEXT) ~ '^\d+$' THEN total_students::INTEGER END AS total_students,
        CASE WHEN BTRIM(student_engagement_percentage::TEXT) ~ '^\d+$' THEN student_engagement_percentage::INTEGER END AS student_engagement_percentage,
        CASE WHEN BTRIM(lesson_planning::TEXT) ~ '^\d+$' THEN lesson_planning::INTEGER END AS lesson_planning,
        CASE WHEN BTRIM(duration_minutes::TEXT) ~ '^\d+$' THEN duration_minutes::INTEGER END AS duration_minutes,
        CASE WHEN BTRIM(teaching_methods::TEXT) ~ '^\d+$' THEN teaching_methods::INTEGER END AS teaching_methods,
        CASE WHEN BTRIM(content_knowledge::TEXT) ~ '^\d+$' THEN content_knowledge::INTEGER END AS content_knowledge,
        CASE WHEN BTRIM(student_engagement::TEXT) ~ '^\d+$' THEN student_engagement::INTEGER END AS student_engagement,
        CASE WHEN BTRIM(classroom_management::TEXT) ~ '^\d+$' THEN classroom_management::INTEGER END AS classroom_management,
        CASE WHEN BTRIM(assessment_techniques::TEXT) ~ '^\d+$' THEN assessment_techniques::INTEGER END AS assessment_techniques,
        COALESCE(BTRIM(specific_feedback::TEXT), '') AS specific_feedback,
        COALESCE(BTRIM(lesson_plan_review::TEXT), '') AS lesson_plan_review,
        COALESCE(BTRIM(learning_objectives::TEXT), '') AS learning_objectives,
        COALESCE(BTRIM(areas_for_improvement::TEXT), '') AS areas_for_improvement
    FROM {{ source('fellowship_school_app_25_26', 'observations_25_26') }}
)

SELECT 
    odc_id,
    academic_year,
    year_map::TEXT AS year_map,
    month,
    CASE
        WHEN LOWER(LEFT(month, 3)) IN ('apr', 'may', 'jun') THEN 'Q1'
        WHEN LOWER(LEFT(month, 3)) IN ('jul', 'aug', 'sep') THEN 'Q2'
        WHEN LOWER(LEFT(month, 3)) IN ('oct', 'nov', 'dec') THEN 'Q3'
        WHEN LOWER(LEFT(month, 3)) IN ('jan', 'feb', 'mar') THEN 'Q4'
    END AS quarter,
    date,
    fellow_id,
    fellow_name,
    pm_id,
    pm_name,
    cohort,
    school,
    city,
    grade,
    grade_section,
    reporting_period,
    grade_observed,
    subject,
    is_completed,
    follow_up_date,
    odc_notes,
    strengths,
    search_fts,
    action_plan,
    lesson_topic,
    total_students,
    student_engagement_percentage,
    lesson_planning,
    duration_minutes,
    teaching_methods,
    content_knowledge,
    student_engagement,
    classroom_management,
    assessment_techniques,
    specific_feedback,
    lesson_plan_review,
    learning_objectives,
    areas_for_improvement
FROM odc
WHERE
    odc_id IS NOT NULL
    AND fellow_id IS NOT NULL
