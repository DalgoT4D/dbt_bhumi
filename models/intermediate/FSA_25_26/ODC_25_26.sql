WITH odc AS (
    SELECT
        COALESCE(BTRIM(id::TEXT), '') AS id_odc,
        COALESCE(INITCAP(BTRIM(city::TEXT)), '') AS city_odc,
        -- Date conversion from DD-MM-YYYY to date
        CASE 
            WHEN date::TEXT ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(date::TEXT, 'DD-MM-YYYY')
        END AS date_odc,
        CASE WHEN BTRIM(cohort::TEXT) ~ '^\d+$' THEN cohort::INTEGER END AS cohort_odc,
        COALESCE(INITCAP(BTRIM(school::TEXT)), '') AS school_odc,
        COALESCE(INITCAP(BTRIM(pm_name::TEXT)), '') AS pm_name_odc,
        COALESCE(INITCAP(BTRIM(subject::TEXT)), '') AS subject_odc,
        COALESCE(BTRIM(fellow_id::TEXT), '') AS fellow_id_odc,
        COALESCE(BTRIM(odc_notes::TEXT), '') AS odc_notes_odc,
        COALESCE(BTRIM(strengths::TEXT), '') AS strengths_odc,
        -- Timestamps
        -- CASE WHEN created_at ~ '^\d{4}-\d{2}-\d{2}' THEN created_at::timestamp END AS created_at_odc,
        -- COALESCE(BTRIM(search_fts::TEXT), '') AS search_fts_odc,
        -- CASE WHEN updated_at ~ '^\d{4}-\d{2}-\d{2}' THEN updated_at::timestamp END AS updated_at_odc,
        COALESCE(BTRIM(action_plan::TEXT), '') AS action_plan_odc,
        COALESCE(INITCAP(BTRIM(fellow_name::TEXT)), '') AS fellow_name_odc,
        COALESCE(is_completed::TEXT IN ('TRUE', 'true', '1'), FALSE) AS is_completed_odc,
        COALESCE(BTRIM(lesson_topic::TEXT), '') AS lesson_topic_odc,
        REGEXP_REPLACE(BTRIM(grade_section::TEXT), '^.*?(\d+).*$','\1') AS grade_section_odc,
        CASE 
            WHEN follow_up_date::TEXT ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(follow_up_date::TEXT, 'DD-MM-YYYY')
        END AS follow_up_date_odc,
        COALESCE(BTRIM(grade_observed::TEXT), '') AS grade_observed_odc,
        CASE WHEN BTRIM(total_students::TEXT) ~ '^\d+$' THEN total_students::INTEGER END AS total_students_odc,
        COALESCE(BTRIM(lesson_planning::TEXT), '') AS lesson_planning_odc,
        CASE WHEN BTRIM(duration_minutes::TEXT) ~ '^\d+$' THEN duration_minutes::INTEGER END AS duration_minutes_odc,
        COALESCE(BTRIM(reporting_period::TEXT), '') AS reporting_period_odc,
        COALESCE(BTRIM(teaching_methods::TEXT), '') AS teaching_methods_odc,
        COALESCE(BTRIM(content_knowledge::TEXT), '') AS content_knowledge_odc,
        COALESCE(BTRIM(specific_feedback::TEXT), '') AS specific_feedback_odc,
        COALESCE(BTRIM(lesson_plan_review::TEXT), '') AS lesson_plan_review_odc,
        COALESCE(BTRIM(student_engagement::TEXT), '') AS student_engagement_odc,
        COALESCE(BTRIM(learning_objectives::TEXT), '') AS learning_objectives_odc,
        COALESCE(BTRIM(classroom_management::TEXT), '') AS classroom_management_odc,
        COALESCE(BTRIM(programme_manager_id::TEXT), '') AS programme_manager_id_odc,
        COALESCE(BTRIM(areas_for_improvement::TEXT), '') AS areas_for_improvement_odc,
        COALESCE(BTRIM(assessment_techniques::TEXT), '') AS assessment_techniques_odc,
        CASE WHEN BTRIM(student_engagement_percentage::TEXT) ~ '^\d+$' THEN student_engagement_percentage::INTEGER END AS student_engagement_percentage_odc
    FROM {{ source('fellowship_school_app_25_26', 'observations_25_26') }}
)

SELECT DISTINCT
    o.id_odc,
    o.city_odc,
    o.date_odc,
    o.cohort_odc,
    o.school_odc,
    o.pm_name_odc,
    o.subject_odc,
    o.fellow_id_odc,
    o.odc_notes_odc,
    o.strengths_odc,
    -- o.created_at_odc,
    -- o.search_fts_odc,
    -- o.updated_at_odc,
    o.action_plan_odc,
    o.fellow_name_odc,
    o.is_completed_odc,
    o.lesson_topic_odc,
    o.grade_section_odc,
    o.follow_up_date_odc,
    o.grade_observed_odc,
    o.total_students_odc,
    o.lesson_planning_odc,
    o.duration_minutes_odc,
    o.reporting_period_odc,
    o.teaching_methods_odc,
    o.content_knowledge_odc,
    o.specific_feedback_odc,
    o.lesson_plan_review_odc,
    o.student_engagement_odc,
    o.learning_objectives_odc,
    o.classroom_management_odc,
    o.programme_manager_id_odc,
    o.areas_for_improvement_odc,
    o.assessment_techniques_odc,
    o.student_engagement_percentage_odc
FROM odc AS o
WHERE o.id_odc <> ''
