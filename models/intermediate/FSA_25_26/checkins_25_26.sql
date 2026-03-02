WITH checkins AS (
    SELECT
        COALESCE(BTRIM(id::TEXT), '') AS id_checkin,
        COALESCE(INITCAP(BTRIM(city::TEXT)), '') AS city_checkin,
        CASE
            WHEN NULLIF(TRIM(date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(date::TEXT)::DATE
        END AS date_checkin,
        COALESCE(BTRIM(notes::TEXT), '') AS notes_checkin,
        CASE WHEN BTRIM(cohort::TEXT) ~ '^\d+$' THEN cohort::INTEGER END AS cohort_checkin,
        COALESCE(INITCAP(BTRIM(school::TEXT)), '') AS school_checkin,
        COALESCE(INITCAP(BTRIM(pm_name::TEXT)), '') AS pm_name_checkin,
        COALESCE(BTRIM(fellow_id::TEXT), '') AS fellow_id_checkin,
        COALESCE(BTRIM(new_goals::TEXT), '') AS new_goals_checkin,
        CASE
            WHEN NULLIF(TRIM(period_from::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(period_from::TEXT)::DATE
        END AS period_from_checkin,
        CASE
            WHEN NULLIF(TRIM(period_to::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(period_to::TEXT)::DATE
        END AS period_to_checkin,
        COALESCE(BTRIM(challenges::TEXT), '') AS challenges_checkin,
        COALESCE(INITCAP(BTRIM(fellow_name::TEXT)), '') AS fellow_name_checkin,
        COALESCE(BTRIM(action_items::TEXT), '') AS action_items_checkin,
        COALESCE(BTRIM(agenda_notes::TEXT), '') AS agenda_notes_checkin,
        COALESCE(is_completed::TEXT IN ('TRUE', 'true', '1'), FALSE) AS is_completed_checkin,
        REGEXP_REPLACE(BTRIM(grade_section::TEXT), '^.*?(\d+).*$','\1') AS grade_section_checkin,
        COALESCE(BTRIM(support_needed::TEXT), '') AS support_needed_checkin,
        CASE WHEN BTRIM(total_students::TEXT) ~ '^\d+$' THEN total_students::INTEGER END AS total_students_checkin,
        COALESCE(BTRIM(lesson_planning::TEXT), '') AS lesson_planning_checkin,
        COALESCE(BTRIM(reporting_period::TEXT), '') AS reporting_period_checkin,
        CASE
            WHEN NULLIF(TRIM(next_checkin_date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(next_checkin_date::TEXT)::DATE
        END AS next_checkin_date_checkin,
        COALESCE(BTRIM(student_engagement::TEXT), '') AS student_engagement_checkin,
        COALESCE(BTRIM(classroom_management::TEXT), '') AS classroom_management_checkin,
        COALESCE(BTRIM(fellow_uploaded_data::TEXT), '') AS fellow_uploaded_data_checkin,
        COALESCE(BTRIM(programme_manager_id::TEXT), '') AS programme_manager_id_checkin,
        COALESCE(BTRIM(previous_goals_review::TEXT), '') AS previous_goals_review_checkin,
        COALESCE(BTRIM(sel_workshop_conducted::TEXT), '') AS sel_workshop_conducted_checkin,
        COALESCE(BTRIM(teaching_effectiveness::TEXT), '') AS teaching_effectiveness_checkin,
        COALESCE(BTRIM(professional_development::TEXT), '') AS professional_development_checkin
    FROM {{ source('fellowship_school_app_25_26', 'checkins_25_26') }}
)

SELECT DISTINCT
    c.id_checkin,
    c.city_checkin,
    c.date_checkin,
    c.notes_checkin,
    c.cohort_checkin,
    c.school_checkin,
    c.pm_name_checkin,
    c.fellow_id_checkin,
    c.new_goals_checkin,
    c.period_to_checkin,
    c.challenges_checkin,
    -- c.created_at_checkin,
    -- c.search_fts_checkin,
    -- c.updated_at_checkin,
    c.fellow_name_checkin,
    c.period_from_checkin,
    c.action_items_checkin,
    c.agenda_notes_checkin,
    c.is_completed_checkin,
    c.grade_section_checkin,
    c.support_needed_checkin,
    c.total_students_checkin,
    c.lesson_planning_checkin,
    c.reporting_period_checkin,
    c.next_checkin_date_checkin,
    c.student_engagement_checkin,
    c.classroom_management_checkin,
    c.fellow_uploaded_data_checkin,
    c.programme_manager_id_checkin,
    c.previous_goals_review_checkin,
    c.sel_workshop_conducted_checkin,
    c.teaching_effectiveness_checkin,
    c.professional_development_checkin
FROM checkins AS c
WHERE c.id_checkin <> ''
