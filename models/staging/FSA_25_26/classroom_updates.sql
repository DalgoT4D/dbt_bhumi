{{ config(
  materialized='table',
  tags=["fsa_25_26", "staging"]
) }}

WITH class_updates AS (
    SELECT 
        COALESCE(BTRIM(id::TEXT), '') AS id,
        CASE 
            WHEN NULLIF(TRIM(reporting_date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(reporting_date::TEXT)::DATE
        END AS reporting_date,
        CASE 
            WHEN NULLIF(TRIM(start_date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(start_date::TEXT)::DATE
        END AS start_date,
        CASE
            WHEN NULLIF(TRIM(start_date::TEXT), '') IS NULL THEN NULL
            WHEN EXTRACT(MONTH FROM TRIM(start_date::TEXT)::DATE) <= 3
                THEN
                    (EXTRACT(YEAR FROM TRIM(start_date::TEXT)::DATE)::INTEGER - 1)::TEXT
                    || ' - ' || EXTRACT(YEAR FROM TRIM(start_date::TEXT)::DATE)::INTEGER::TEXT
            ELSE
                EXTRACT(YEAR FROM TRIM(start_date::TEXT)::DATE)::INTEGER::TEXT
                || ' - ' || (EXTRACT(YEAR FROM TRIM(start_date::TEXT)::DATE)::INTEGER + 1)::TEXT
        END AS academic_year,
        CASE
            WHEN NULLIF(TRIM(start_date::TEXT), '') IS NULL THEN NULL
            WHEN EXTRACT(MONTH FROM TRIM(start_date::TEXT)::DATE) <= 3
                THEN
                    EXTRACT(YEAR FROM TRIM(start_date::TEXT)::DATE)::INTEGER - 1
            ELSE
                EXTRACT(YEAR FROM TRIM(start_date::TEXT)::DATE)::INTEGER
        END AS year_map,
        CASE
            WHEN NULLIF(TRIM(start_date::TEXT), '') IS NULL THEN NULL
            ELSE TO_CHAR(TRIM(start_date::TEXT)::DATE, 'Mon')
        END AS month,
        CASE 
            WHEN NULLIF(TRIM(end_date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(end_date::TEXT)::DATE
        END AS end_date,
        COALESCE(BTRIM(fellow_id::TEXT), '') AS fellow_id,
        COALESCE(BTRIM(fellow_name::TEXT), '') AS fellow_name,
        COALESCE(BTRIM(pm_name::TEXT), '') AS pm_name,
        NULLIF(BTRIM(cohort_year::TEXT),'') AS cohort,
        COALESCE(INITCAP(BTRIM(school_name::TEXT)), '') AS school_name,
        COALESCE(BTRIM(city::TEXT), '') AS city,
        REGEXP_REPLACE(BTRIM(grade_section::TEXT), '[^0-9]', '', 'g') AS grade,
        LOWER(COALESCE(BTRIM(grade_section::TEXT), '')) AS grade_section,
        CASE WHEN BTRIM(stem_score::TEXT) ~ '^[0-9.]+$' THEN stem_score::NUMERIC END AS stem_score,
        CASE WHEN BTRIM(ptms::TEXT) ~ '^\d+$' THEN ptms::INTEGER END AS ptms,
        CASE WHEN BTRIM(helo_circles::TEXT) ~ '^[0-9.]+$' THEN helo_circles::NUMERIC END AS helo_circles,
        CASE WHEN BTRIM(homes_visited::TEXT) ~ '^[0-9.]+$' THEN homes_visited::NUMERIC END AS homes_visited,
        CASE WHEN BTRIM(teaching_hours::TEXT) ~ '^[0-9.]+$' THEN teaching_hours::NUMERIC END AS teaching_hours,
        CASE WHEN BTRIM(total_students::TEXT) ~ '^[0-9.]+$' THEN total_students::NUMERIC END AS total_students,
        CASE WHEN BTRIM(teacher_circles::TEXT) ~ '^[0-9.]+$' THEN teacher_circles::NUMERIC END AS teacher_circles,
        COALESCE(BTRIM(reporting_period::TEXT), '') AS reporting_period,
        COALESCE(BTRIM(helo_lesson_names::TEXT), '') AS helo_lesson_names,
        CASE WHEN BTRIM(mathematics_score::TEXT) ~ '^[0-9.]+$' THEN mathematics_score::NUMERIC END AS mathematics_score,
        CASE WHEN BTRIM(school_leader_checkins::TEXT) ~ '^[0-9.]+$' THEN school_leader_checkins::NUMERIC END AS school_leader_checkins,
        CASE WHEN BTRIM(reading_comprehension_score::TEXT) ~ '^[0-9.]+$' THEN reading_comprehension_score::NUMERIC END AS reading_comprehension_score
    FROM {{ source('fellowship_school_app_25_26', 'fellow_classroom_updates_25_26') }}
)

SELECT
    id,
    academic_year,
    year_map::TEXT AS year_map,
    month,
    CASE
        WHEN LOWER(LEFT(month, 3)) IN ('apr', 'may', 'jun') THEN 'Q1'
        WHEN LOWER(LEFT(month, 3)) IN ('jul', 'aug', 'sep') THEN 'Q2'
        WHEN LOWER(LEFT(month, 3)) IN ('oct', 'nov', 'dec') THEN 'Q3'
        WHEN LOWER(LEFT(month, 3)) IN ('jan', 'feb', 'mar') THEN 'Q4'
    END AS quarter,
    reporting_date,
    start_date,
    end_date,
    fellow_id,
    fellow_name,
    pm_name,
    cohort,
    school_name,
    city,
    grade,
    grade_section,
    stem_score,
    ptms,
    helo_circles,
    homes_visited,
    teaching_hours,
    total_students,
    teacher_circles,
    reporting_period,
    helo_lesson_names,
    mathematics_score,
    school_leader_checkins,
    reading_comprehension_score
FROM class_updates
WHERE
    id IS NOT NULL
    AND fellow_id IS NOT NULL
