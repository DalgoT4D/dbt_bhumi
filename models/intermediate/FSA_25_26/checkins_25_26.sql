WITH checkins AS (
    SELECT
        COALESCE(BTRIM(id::TEXT), '') AS id,
        CASE
            WHEN NULLIF(TRIM(date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(date::TEXT)::DATE
        END AS date,
        CASE WHEN BTRIM(cohort::TEXT) ~ '^\d+$' THEN cohort::INTEGER END AS cohort,
        COALESCE(INITCAP(BTRIM(school::TEXT)), '') AS school,
        COALESCE(INITCAP(BTRIM(pm_name::TEXT)), '') AS pm_name,
        COALESCE(BTRIM(fellow_id::TEXT), '') AS fellow_id,
        CASE
            WHEN NULLIF(TRIM(period_from::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(period_from::TEXT)::DATE
        END AS period_from,
        TO_CHAR(period_from, 'Mon YYYY') AS month_year,
        CASE
            WHEN NULLIF(TRIM(period_to::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(period_to::TEXT)::DATE
        END AS period_to,
        COALESCE(INITCAP(BTRIM(fellow_name::TEXT)), '') AS fellow_name,
        COALESCE(is_completed::TEXT IN ('TRUE', 'true', '1'), FALSE) AS is_completed,
        REGEXP_REPLACE(BTRIM(grade_section::TEXT), '^.*?(\d+).*$','\1') AS grade,
        CASE WHEN BTRIM(total_students::TEXT) ~ '^\d+$' THEN total_students::INTEGER END AS total_students,
        COALESCE(BTRIM(reporting_period::TEXT), '') AS reporting_period,
        CASE
            WHEN NULLIF(TRIM(next_checkin_date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(next_checkin_date::TEXT)::DATE
        END AS next_checkin_date,
        COALESCE(BTRIM(student_engagement::TEXT), '') AS student_engagement,
        COALESCE(BTRIM(classroom_management::TEXT), '') AS classroom_management,
        COALESCE(BTRIM(fellow_uploaded_data::TEXT), '') AS fellow_uploaded_data,
        COALESCE(BTRIM(programme_manager_id::TEXT), '') AS pm_id,
        COALESCE(BTRIM(sel_workshop_conducted::TEXT), '') AS sel_workshop_conducted,
        COALESCE(BTRIM(teaching_effectiveness::TEXT), '') AS teaching_effectiveness,
        COALESCE(BTRIM(professional_development::TEXT), '') AS professional_development

        -- COALESCE(INITCAP(BTRIM(city::TEXT)), '') AS city,
        -- COALESCE(BTRIM(notes::TEXT), '') AS notes,
        -- COALESCE(BTRIM(new_goals::TEXT), '') AS new_goals,
        -- COALESCE(BTRIM(challenges::TEXT), '') AS challenges,
        -- COALESCE(BTRIM(action_items::TEXT), '') AS action_items,
        -- COALESCE(BTRIM(agenda_notes::TEXT), '') AS agenda_notes,
        -- COALESCE(BTRIM(support_needed::TEXT), '') AS support_needed,
        -- COALESCE(BTRIM(lesson_planning::TEXT), '') AS lesson_planning,
        -- COALESCE(BTRIM(previous_goals_review::TEXT), '') AS previous_goals_review,
    FROM {{ source('fellowship_school_app_25_26', 'checkins_25_26') }}
),

fellow_school as (
    select
        fellow_id,
        school_id,
        school_state,
        school_district as city,
        udise_code,
        school_type,
        year_1_donor,
        year_2_donor
    from {{ ref('fellow_school_25_26') }}
)

SELECT DISTINCT
    c.id,
    c.date,
    c.cohort,
    c.school,
    fs.school_id,
    fs.school_state,
    fs.city,
    fs.udise_code,
    fs.school_type,
    c.grade,
    c.pm_id,
    c.pm_name,
    fs.fellow_id,
    fs.year_1_donor,
    fs.year_2_donor,
    c.period_from,
    c.month_year,
    c.period_to,
    c.fellow_name,
    c.is_completed,
    c.total_students,
    c.reporting_period,
    c.next_checkin_date,
    c.student_engagement,
    c.classroom_management,
    c.fellow_uploaded_data,
    c.sel_workshop_conducted,
    c.teaching_effectiveness,
    c.professional_development
FROM checkins AS c
LEFT JOIN fellow_school AS fs
    ON c.fellow_id = fs.fellow_id
WHERE c.id IS NOT NULL
    and fs.fellow_id IS NOT NULL
