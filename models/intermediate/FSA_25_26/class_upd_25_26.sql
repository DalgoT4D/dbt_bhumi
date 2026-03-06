WITH class_updates AS (
    SELECT 
        COALESCE(BTRIM(cu.id::TEXT), '') AS id,
        COALESCE(INITCAP(BTRIM(cu.city::TEXT)), '') AS city,
        CASE 
            WHEN NULLIF(TRIM(cu.reporting_date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(cu.reporting_date::TEXT)::DATE
        END AS reporting_date,
        CASE 
            WHEN NULLIF(TRIM(cu.start_date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(cu.start_date::TEXT)::DATE
        END AS start_date,
        TO_CHAR(cu.start_date, 'Mon YYYY') AS month_year,
        CASE 
            WHEN NULLIF(TRIM(cu.end_date::TEXT), '') IS NULL THEN NULL
            ELSE TRIM(cu.end_date::TEXT)::DATE
        END AS end_date,
        COALESCE(BTRIM(cu.fellow_id::TEXT), '') AS fellow_id,
        CASE WHEN BTRIM(cu.stem_score::TEXT) ~ '^[0-9.]+$' THEN cu.stem_score::NUMERIC END AS stem_score,
        NULLIF(BTRIM(cu.cohort_year::TEXT),'') AS cohort_year,
        COALESCE(INITCAP(BTRIM(cu.fellow_name::TEXT)), '') AS fellow_name,
        COALESCE(INITCAP(BTRIM(cu.school_name::TEXT)), '') AS school_name,
        CASE WHEN BTRIM(cu.helo_circles::TEXT) ~ '^[0-9.]+$' THEN cu.helo_circles::NUMERIC END AS helo_circles,
        REGEXP_REPLACE(BTRIM(cu.grade_section::TEXT), '[^0-9]', '', 'g') AS grade,
        CASE WHEN BTRIM(cu.ptms::TEXT) ~ '^\d+$' THEN cu.ptms::INTEGER END AS ptms,
        CASE WHEN BTRIM(cu.homes_visited::TEXT) ~ '^[0-9.]+$' THEN cu.homes_visited::NUMERIC END AS homes_visited,
        CASE WHEN BTRIM(cu.teaching_hours::TEXT) ~ '^[0-9.]+$' THEN cu.teaching_hours::NUMERIC END AS teaching_hours,
        CASE WHEN BTRIM(cu.total_students::TEXT) ~ '^[0-9.]+$' THEN cu.total_students::NUMERIC END AS total_students,
        CASE WHEN BTRIM(cu.teacher_circles::TEXT) ~ '^[0-9.]+$' THEN cu.teacher_circles::NUMERIC END AS teacher_circles,
        COALESCE(BTRIM(cu.reporting_period::TEXT), '') AS reporting_period,
        COALESCE(BTRIM(cu.helo_lesson_names::TEXT), '') AS helo_lesson_names,
        CASE WHEN BTRIM(cu.mathematics_score::TEXT) ~ '^[0-9.]+$' THEN cu.mathematics_score::NUMERIC END AS mathematics_score,
        CASE WHEN BTRIM(cu.school_leader_checkins::TEXT) ~ '^[0-9.]+$' THEN cu.school_leader_checkins::NUMERIC END AS school_leader_checkins,
        CASE WHEN BTRIM(cu.reading_comprehension_score::TEXT) ~ '^[0-9.]+$' THEN cu.reading_comprehension_score::NUMERIC END AS reading_comprehension_score
    FROM {{ source('fellowship_school_app_25_26', 'fellow_classroom_updates_25_26') }} AS cu
),

fellow_school AS (
    SELECT 
        fellow_id,
        cohort_year,
        year_1_donor,
        year_2_donor,
        school_id,
        school_name,
        school_state,
        udise_code,
        school_type,
        grade,
        pm_id,
        pm_full_name
    FROM {{ ref('fellow_school_25_26') }}
)

SELECT DISTINCT
    cu.id,
    cu.city,
    cu.reporting_date,
    cu.start_date,
    cu.month_year,
    cu.end_date,
    fs.fellow_id,
    cu.stem_score,
    cu.cohort_year,
    cu.fellow_name,
    cu.school_name,
    cu.helo_circles,
    cu.grade,
    cu.ptms,
    cu.homes_visited,
    cu.teaching_hours,
    cu.total_students,
    cu.teacher_circles,
    cu.reporting_period,
    cu.helo_lesson_names,
    cu.mathematics_score,
    cu.school_leader_checkins,
    cu.reading_comprehension_score,
    fs.year_1_donor,
    fs.year_2_donor,
    fs.school_id,
    fs.school_state,
    fs.udise_code,
    fs.school_type,
    fs.pm_id,
    fs.pm_full_name
FROM class_updates AS cu
LEFT JOIN fellow_school AS fs
    ON cu.fellow_id = fs.fellow_id
WHERE
    cu.id IS NOT NULL 
    AND fs.fellow_id IS NOT NULL
