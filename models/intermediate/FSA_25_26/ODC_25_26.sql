WITH odc AS (
    SELECT
        COALESCE(BTRIM(id::TEXT), '') AS id,
        -- Date conversion from DD-MM-YYYY to date
        CASE
            WHEN NULLIF(TRIM(date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(date::TEXT)::DATE
        END AS date,
        TO_CHAR(date, 'Mon YYYY') AS month_year,
        CASE WHEN BTRIM(cohort::TEXT) ~ '^\d+$' THEN cohort::INTEGER END AS cohort,
        COALESCE(INITCAP(BTRIM(school::TEXT)), '') AS school,
        COALESCE(BTRIM(programme_manager_id::TEXT), '') AS pm_id,
        COALESCE(INITCAP(BTRIM(pm_name::TEXT)), '') AS pm_name,
        COALESCE(BTRIM(fellow_id::TEXT), '') AS fellow_id,
        COALESCE(INITCAP(BTRIM(fellow_name::TEXT)), '') AS fellow_name,
        COALESCE(is_completed::TEXT IN ('TRUE', 'true', '1'), FALSE) AS is_completed,
        REGEXP_REPLACE(BTRIM(grade_section::TEXT), '[^0-9]', '', 'g') AS grade_section,
        REGEXP_REPLACE(BTRIM(grade_observed::TEXT), '[^0-9]', '', 'g') AS grade_observed,
        CASE
            WHEN NULLIF(TRIM(follow_up_date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(follow_up_date::TEXT)::DATE
        END AS follow_up_date,
        CASE WHEN BTRIM(total_students::TEXT) ~ '^\d+$' THEN total_students::INTEGER END AS total_students,
        CASE WHEN BTRIM(duration_minutes::TEXT) ~ '^\d+$' THEN duration_minutes::INTEGER END AS duration_minutes,
        COALESCE(BTRIM(reporting_period::TEXT), '') AS reporting_period,
        CASE WHEN BTRIM(teaching_methods::TEXT) ~ '^\d+$' THEN teaching_methods::INTEGER END AS teaching_methods,
        CASE WHEN BTRIM(content_knowledge::TEXT) ~ '^\d+$' THEN content_knowledge::INTEGER END AS content_knowledge,
        CASE WHEN BTRIM(student_engagement::TEXT) ~ '^\d+$' THEN student_engagement::INTEGER END AS student_engagement,
        CASE WHEN BTRIM(classroom_management::TEXT) ~ '^\d+$' THEN classroom_management::INTEGER END AS classroom_management,
        CASE WHEN BTRIM(assessment_techniques::TEXT) ~ '^\d+$' THEN assessment_techniques::INTEGER END AS assessment_techniques,
        CASE WHEN BTRIM(student_engagement_percentage::TEXT) ~ '^\d+$' THEN student_engagement_percentage::INTEGER END AS student_engagement_percentage

        -- COALESCE(BTRIM(learning_objectives::TEXT), '') AS learning_objectives,
        -- COALESCE(BTRIM(lesson_planning::TEXT), '') AS lesson_planning,

        -- COALESCE(INITCAP(BTRIM(city::TEXT)), '') AS city,
        -- COALESCE(INITCAP(BTRIM(subject::TEXT)), '') AS subject,
        -- COALESCE(BTRIM(odc_notes::TEXT), '') AS odc_notes,
        -- COALESCE(BTRIM(strengths::TEXT), '') AS strengths,
        -- COALESCE(BTRIM(action_plan::TEXT), '') AS action_plan,
        -- COALESCE(BTRIM(lesson_topic::TEXT), '') AS lesson_topic,
        -- COALESCE(BTRIM(specific_feedback::TEXT), '') AS specific_feedback,
        -- COALESCE(BTRIM(lesson_plan_review::TEXT), '') AS lesson_plan_review,
        -- COALESCE(BTRIM(areas_for_improvement::TEXT), '') AS areas_for_improvement,
    FROM {{ source('fellowship_school_app_25_26', 'observations_25_26') }}
),

fellow_school AS (
    SELECT
        fellow_id,
        fellow_name,
        cohort,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        year_1_donor,
        year_2_donor,
        pm_id,
        pm_name,
        no_of_students
    FROM {{ ref('fellow_school_25_26') }}
)

SELECT DISTINCT
    o.id,
    o.date,
    o.month_year,
    fs.school_id,
    fs.school_name,
    fs.cohort,
    fs.school_state,
    fs.school_district,
    fs.udise_code,
    fs.school_type,
    fs.grade,
    fs.fellow_id,
    fs.fellow_name,
    fs.year_1_donor,
    fs.year_2_donor,
    fs.pm_id,
    fs.pm_name,
    o.grade_section,
    o.grade_observed,
    o.is_completed,
    o.follow_up_date,
    o.total_students,
    fs.no_of_students,
    o.duration_minutes,
    o.reporting_period,
    -- o.lesson_planning,
    o.teaching_methods,
    o.content_knowledge,
    o.student_engagement,
    -- o.learning_objectives,
    o.classroom_management,
    o.assessment_techniques,
    o.student_engagement_percentage
FROM odc AS o
LEFT JOIN fellow_school AS fs
    ON o.fellow_id = fs.fellow_id
WHERE
    o.id IS NOT NULL
    AND fs.fellow_id IS NOT NULL
