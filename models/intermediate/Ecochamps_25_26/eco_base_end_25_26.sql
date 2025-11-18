WITH base AS (
    SELECT
        "Roll No",
        "Chapter",
        "School",
        "Grade",
        "School ID",
        "Student Name",
        "Student Status",
        "Donor Mapped",

        -- Baseline score (already present in source)
        "Baseline Score" AS baseline_score,
        "Baseline" AS baseline_raw,

        -- Endline score / raw
        "Endline Score" AS endline_score,
        "Endline" AS endline_raw,

        -- Baseline attendance: P->Present, A->Absent, numeric-start -> Present
        CASE
            WHEN "Baseline" ~ '^P' THEN 'Present'
            WHEN "Baseline" ~ '^A' THEN 'Absent'
            WHEN "Baseline" ~ '^\s*\d' THEN 'Present'
            ELSE NULL
        END AS baseline_attendance,

        -- Baseline date only when parentheses exist
        CASE
            WHEN "Baseline" ~ '\('
            THEN NULLIF(TRIM(REGEXP_REPLACE("Baseline", '^[^\(]*\((.*?)\).*$', '\1')), '')
            ELSE NULL
        END AS baseline_date_text,

        -- Endline attendance: same logic
        CASE
            WHEN "Endline" ~ '^P' THEN 'Present'
            WHEN "Endline" ~ '^A' THEN 'Absent'
            WHEN "Endline" ~ '^\s*\d' THEN 'Present'
            ELSE NULL
        END AS endline_attendance,

        CASE
            WHEN "Endline" ~ '\('
            THEN NULLIF(TRIM(REGEXP_REPLACE("Endline", '^[^\(]*\((.*?)\).*$', '\1')), '')
            ELSE NULL
        END AS endline_date_text

    FROM {{ ref('eco_student25_26_stg') }}
),

parsed_dates AS (
    SELECT
        *,
        -- parse baseline_date_text to true date
        CASE
            WHEN baseline_date_text IS NOT NULL THEN
                CASE
                    WHEN baseline_date_text ~ '^\d{1,2}\s+[A-Za-z]{3}\s+\d{4}$' THEN TO_DATE(baseline_date_text, 'DD Mon YYYY')
                    WHEN baseline_date_text ~ '^[A-Za-z]{3}\s+\d{1,2}\s+\d{4}$' THEN TO_DATE(baseline_date_text, 'Mon DD YYYY')
                    WHEN baseline_date_text ~ '^\d{1,2}/\d{1,2}/\d{2}$' THEN TO_DATE(baseline_date_text, 'DD/MM/YY')
                    WHEN baseline_date_text ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(baseline_date_text, 'DD/MM/YYYY')
                    ELSE NULL
                END
            ELSE NULL
        END AS baseline_date_parsed,

        -- parse endline_date_text to true date
        CASE
            WHEN endline_date_text IS NOT NULL THEN
                CASE
                    WHEN endline_date_text ~ '^\d{1,2}\s+[A-Za-z]{3}\s+\d{4}$' THEN TO_DATE(endline_date_text, 'DD Mon YYYY')
                    WHEN endline_date_text ~ '^[A-Za-z]{3}\s+\d{1,2}\s+\d{4}$' THEN TO_DATE(endline_date_text, 'Mon DD YYYY')
                    WHEN endline_date_text ~ '^\d{1,2}/\d{1,2}/\d{2}$' THEN TO_DATE(endline_date_text, 'DD/MM/YY')
                    WHEN endline_date_text ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(endline_date_text, 'DD/MM/YYYY')
                    ELSE NULL
                END
            ELSE NULL
        END AS endline_date_parsed

    FROM base
)

SELECT
    "Roll No",
    "Chapter",
    "School",
    "Grade",
    "School ID",
    "Student Name",
    "Student Status",
    "Donor Mapped",

    -- Baseline fields
    baseline_score,
    baseline_attendance,
    baseline_date_parsed AS baseline_date,

    -- Endline fields
    endline_score,
    endline_attendance,
    endline_date_parsed AS endline_date,

    NULLIF(
        (
            (CASE WHEN baseline_attendance IS NOT NULL AND TRIM(baseline_attendance) <> '' THEN 1 ELSE 0 END)
            + (CASE WHEN endline_attendance IS NOT NULL AND TRIM(endline_attendance) <> '' THEN 1 ELSE 0 END)
        ),
        0
    ) AS "Assessment completed",

    -- Assessment Attendance %: percent of Present across baseline & endline,
    -- denominator = number of non-NULL assessments (baseline/endline)
    CASE
        WHEN (
            (CASE WHEN baseline_attendance IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN endline_attendance IS NOT NULL THEN 1 ELSE 0 END)
        ) = 0 THEN NULL
        ELSE ROUND(
            (
                (CASE WHEN baseline_attendance = 'Present' THEN 1 ELSE 0 END) +
                (CASE WHEN endline_attendance = 'Present' THEN 1 ELSE 0 END)
            )::NUMERIC
            /
            (
                (CASE WHEN baseline_attendance IS NOT NULL THEN 1 ELSE 0 END) +
                (CASE WHEN endline_attendance IS NOT NULL THEN 1 ELSE 0 END)
            ) * 100
        , 2)
    END AS "Assessment Attendance %"

FROM parsed_dates
