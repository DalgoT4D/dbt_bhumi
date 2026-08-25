{{ config(
  materialized='table',
  tags=["fsa_25_26", "staging"]
) }}

WITH checkins AS (
    SELECT
        COALESCE(BTRIM(id::TEXT), '') AS checkin_id,
        COALESCE(BTRIM(city::TEXT), '') AS city,
        date::DATE AS date,
        COALESCE(BTRIM(notes::TEXT), '') AS notes,
        COALESCE(BTRIM(cohort::TEXT), '') AS cohort,
        COALESCE(BTRIM(fellow_id::TEXT), '') AS fellow_id,
        COALESCE(BTRIM(school::TEXT), '') AS school,
        COALESCE(BTRIM(programme_manager_id::TEXT), '') AS pm_id,
        COALESCE(BTRIM(pm_name::TEXT), '') AS pm_name,
        COALESCE(BTRIM(fellow_name::TEXT), '') AS fellow_name,
        COALESCE(BTRIM(new_goals::TEXT), '') AS new_goals,
        CASE
            WHEN NULLIF(TRIM(period_from::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(period_from::TEXT)::DATE
        END AS period_from,
        CASE
            WHEN NULLIF(TRIM(period_from::TEXT), '') IS NULL THEN NULL
            ELSE TO_CHAR(TRIM(period_from::TEXT)::DATE, 'Mon')
        END AS month,
        CASE
            WHEN NULLIF(TRIM(period_from::TEXT), '') IS NULL THEN NULL
            WHEN EXTRACT(MONTH FROM TRIM(period_from::TEXT)::DATE) <= 3
                THEN
                    (EXTRACT(YEAR FROM TRIM(period_from::TEXT)::DATE)::INTEGER - 1)::TEXT
                    || ' - ' || EXTRACT(YEAR FROM TRIM(period_from::TEXT)::DATE)::INTEGER::TEXT
            ELSE
                EXTRACT(YEAR FROM TRIM(period_from::TEXT)::DATE)::INTEGER::TEXT
                || ' - ' || (EXTRACT(YEAR FROM TRIM(period_from::TEXT)::DATE)::INTEGER + 1)::TEXT
        END AS academic_year,
        CASE
            WHEN NULLIF(TRIM(period_from::TEXT), '') IS NULL THEN NULL
            WHEN EXTRACT(MONTH FROM TRIM(period_from::TEXT)::DATE) <= 3
                THEN
                    EXTRACT(YEAR FROM TRIM(period_from::TEXT)::DATE)::INTEGER - 1
            ELSE
                EXTRACT(YEAR FROM TRIM(period_from::TEXT)::DATE)::INTEGER
        END AS year_map,      
        CASE
            WHEN NULLIF(TRIM(period_to::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(period_to::TEXT)::DATE
        END AS period_to,
        COALESCE(BTRIM(challenges::TEXT), '') AS challenges,
        COALESCE(BTRIM(search_fts::TEXT), '') AS search_fts,
        COALESCE(BTRIM(action_items::TEXT), '') AS action_items,
        COALESCE(BTRIM(agenda_notes::TEXT), '') AS agenda_notes,
        is_completed,
        REGEXP_REPLACE(BTRIM(grade_section::TEXT), '^.*?(\d+).*$','\1') AS grade,
        LOWER(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(BTRIM(grade_section::TEXT), ''), '-', '', 'g'), '([0-9])([a-zA-Z])', '\1 \2', 'g')) AS grade_section,
        COALESCE(BTRIM(support_needed::TEXT), '') AS support_needed,
        COALESCE(BTRIM(reporting_period::TEXT), '') AS reporting_period,
        CASE
            WHEN NULLIF(TRIM(next_checkin_date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(next_checkin_date::TEXT)::DATE
        END AS next_checkin_date,
        CASE
            WHEN NULLIF(BTRIM(total_students::TEXT), '') IS NULL THEN NULL
            ELSE BTRIM(total_students::TEXT)::INTEGER
        END AS total_students,
        CASE WHEN BTRIM(lesson_planning::TEXT) ~ '^\d+$' THEN lesson_planning::INTEGER END AS lesson_planning,
        fellow_uploaded_data,
        sel_workshop_conducted,
        COALESCE(BTRIM(previous_goals_review::TEXT), '') AS previous_goals_review,
        CASE WHEN BTRIM(student_engagement::TEXT) ~ '^\d+$' THEN student_engagement::INTEGER END AS student_engagement,
        CASE WHEN BTRIM(classroom_management::TEXT) ~ '^\d+$' THEN classroom_management::INTEGER END AS classroom_management,
        CASE WHEN BTRIM(teaching_effectiveness::TEXT) ~ '^\d+$' THEN teaching_effectiveness::INTEGER END AS teaching_effectiveness,
        CASE WHEN BTRIM(professional_development::TEXT) ~ '^\d+$' THEN professional_development::INTEGER END AS professional_development

    FROM {{ source('fellowship_school_app_25_26', 'checkins_25_26') }}
)

SELECT 
    academic_year,
    checkin_id,
    city,
    date,
    notes,
    fellow_id,
    cohort,
    school,
    pm_id,
    pm_name,
    fellow_name,
    new_goals,
    period_from,
    month,
    year_map::TEXT AS year_map,
    CASE
        WHEN LOWER(LEFT(month, 3)) IN ('apr', 'may', 'jun') THEN 'Q1'
        WHEN LOWER(LEFT(month, 3)) IN ('jul', 'aug', 'sep') THEN 'Q2'
        WHEN LOWER(LEFT(month, 3)) IN ('oct', 'nov', 'dec') THEN 'Q3'
        WHEN LOWER(LEFT(month, 3)) IN ('jan', 'feb', 'mar') THEN 'Q4'
    END AS quarter,
    period_to,
    challenges,
    search_fts,
    action_items,
    agenda_notes,
    is_completed,
    grade,
    grade_section,
    support_needed,
    reporting_period,
    next_checkin_date,
    total_students,
    lesson_planning,
    fellow_uploaded_data,
    sel_workshop_conducted,
    previous_goals_review,
    student_engagement,
    classroom_management,
    teaching_effectiveness,
    professional_development
FROM checkins
WHERE
    checkin_id IS NOT NULL
    AND fellow_id IS NOT NULL
