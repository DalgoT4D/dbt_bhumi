SELECT DISTINCT
    COALESCE(BTRIM(cu.id::TEXT), '') AS id,
    COALESCE(INITCAP(BTRIM(cu.city::TEXT)), '') AS city,
    CASE WHEN BTRIM(cu.ptms::TEXT) ~ '^\d+$' THEN cu.ptms::INTEGER END AS ptms,
    COALESCE(INITCAP(BTRIM(sfpc.pm_full_name)), INITCAP(BTRIM(cu.pm_name::TEXT)), '') AS pm_name,
    CASE 
        WHEN cu.end_date::TEXT ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(cu.end_date::TEXT, 'DD-MM-YYYY')
        ELSE NULL
    END AS end_date,
    COALESCE(BTRIM(cu.fellow_id::TEXT), '') AS fellow_id,
    -- CASE WHEN cu.created_at::TEXT ~ '^\d{4}-\d{2}-\d{2}' THEN cu.created_at::timestamp END AS created_at,
    CASE 
        WHEN cu.start_date::TEXT ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(cu.start_date::TEXT, 'DD-MM-YYYY')
        ELSE NULL
    END AS start_date,
    CASE WHEN BTRIM(cu.stem_score::TEXT) ~ '^[0-9.]+$' THEN cu.stem_score::NUMERIC END AS stem_score,
    -- CASE WHEN cu.updated_at::TEXT ~ '^\\d{4}-\\d{2}-\\d{2}' THEN cu.updated_at::timestamp END AS updated_at,
    CASE WHEN BTRIM(cu.cohort_year::TEXT) ~ '^\\d+$' THEN cu.cohort_year::INTEGER END AS cohort_year,
    COALESCE(INITCAP(BTRIM(cu.fellow_name::TEXT)), '') AS fellow_name,
    COALESCE(INITCAP(BTRIM(cu.school_name::TEXT)), '') AS school_name,
    CASE WHEN BTRIM(cu.helo_circles::TEXT) ~ '^\\d+$' THEN cu.helo_circles::INTEGER END AS helo_circles,
    REGEXP_REPLACE(BTRIM(cu.grade_section::TEXT), '^.*?(\\d+).*$','\\1') AS grade_section,
    CASE WHEN BTRIM(cu.homes_visited::TEXT) ~ '^\\d+$' THEN cu.homes_visited::INTEGER END AS homes_visited,
    CASE 
        WHEN cu.reporting_date::TEXT ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(cu.reporting_date::TEXT, 'DD-MM-YYYY')
        ELSE NULL
    END AS reporting_date,
    CASE WHEN BTRIM(cu.teaching_hours::TEXT) ~ '^[0-9.]+$' THEN cu.teaching_hours::NUMERIC END AS teaching_hours,
    CASE WHEN BTRIM(cu.total_students::TEXT) ~ '^\\d+$' THEN cu.total_students::INTEGER END AS total_students,
    CASE WHEN BTRIM(cu.teacher_circles::TEXT) ~ '^\\d+$' THEN cu.teacher_circles::INTEGER END AS teacher_circles,
    COALESCE(BTRIM(cu.reporting_period::TEXT), '') AS reporting_period,
    COALESCE(BTRIM(cu.helo_lesson_names::TEXT), '') AS helo_lesson_names,
    CASE WHEN BTRIM(cu.mathematics_score::TEXT) ~ '^[0-9.]+$' THEN cu.mathematics_score::NUMERIC END AS mathematics_score,
    CASE WHEN BTRIM(cu.school_leader_checkins::TEXT) ~ '^\\d+$' THEN cu.school_leader_checkins::INTEGER END AS school_leader_checkins,
    CASE WHEN BTRIM(cu.reading_comprehension_score::TEXT) ~ '^[0-9.]+$' THEN cu.reading_comprehension_score::NUMERIC END AS reading_comprehension_score
FROM {{ source('fellowship_school_app_25_26', 'fellow_classroom_updates_25_26') }} cu
LEFT JOIN {{ ref('SFPC_25_26') }} sfpc 
    ON sfpc.fellow_id = cu.fellow_id
WHERE COALESCE(BTRIM(cu.id::TEXT), '') <> ''
