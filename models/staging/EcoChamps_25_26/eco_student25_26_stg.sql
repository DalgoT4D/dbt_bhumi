WITH clean AS (
    SELECT
        -- Basic Info
        COALESCE(INITCAP(BTRIM("Chapter"::TEXT)), '') AS "Chapter",
        COALESCE(INITCAP(BTRIM("School"::TEXT)), '') AS "School",
        COALESCE(BTRIM("Grade"::TEXT), '') AS "Grade",
        CASE WHEN BTRIM("School_ID"::TEXT) ~ '^\d+$' THEN ("School_ID"::TEXT)::BIGINT ELSE NULL END AS "School ID",
        COALESCE(INITCAP(BTRIM("_Student_Name_"::TEXT)), '') AS "Student Name",
        COALESCE(INITCAP(BTRIM("Center_Coordiantor__1_"::TEXT)), '') AS "Center Coordiantor (1)",
        COALESCE(INITCAP(BTRIM("Center_Coordianator__2_"::TEXT)), '') AS "Center Coordianator (2)",
        COALESCE(BTRIM("Student_Status"::TEXT), '') AS "Student Status",

        -- Generate a random 10-character alphanumeric unique ID
        upper(substring(replace(gen_random_uuid()::text, '-', '') from 1 for 10)) as "Student Unique ID",

        -- Baseline & Scores
        CASE 
            WHEN BTRIM("Baseline_Score"::TEXT) ~ '^[0-9]+$' 
            THEN ("Baseline_Score"::TEXT)::INT 
            ELSE NULL 
        END AS "Baseline Score",

        COALESCE(BTRIM("Baseline"::TEXT), '') AS "Baseline",
        COALESCE(BTRIM("Kitchen_Garden"::TEXT), '') AS "Kitchen Garden",
        COALESCE(BTRIM("Waste_Management"::TEXT), '') AS "Waste Management",
        COALESCE(BTRIM("Water_Conservation"::TEXT), '') AS "Water Conservation",
        COALESCE(BTRIM("Climate"::TEXT), '') AS "Climate",
        COALESCE(BTRIM("Life_Style___Choices"::TEXT), '') AS "Life Style & Choices",
        COALESCE(BTRIM("Endline"::TEXT), '') AS "Endline",

        -- Additional Metrics
        CASE 
            WHEN BTRIM("Modlues_Completed"::TEXT) ~ '^\d+$' 
            THEN ("Modlues_Completed"::TEXT)::INT 
            ELSE 0 
        END AS "Modlues Completed",

        CASE 
            WHEN BTRIM("Attendance__"::TEXT) ~ '^[0-9.]+%?$' 
            THEN REPLACE("Attendance__", '%', '')::NUMERIC 
            ELSE NULL 
        END AS "Attendance %"

    FROM {{ source('ecochamps25_26', 'Student___Session_Data') }}
)

SELECT DISTINCT
    "Student Unique ID",
    "Chapter",
    "School",
    "Grade",
    "School ID",
    "Student Name",
    "Center Coordiantor (1)",
    "Center Coordianator (2)",
    "Student Status",
    "Baseline Score",
    "Baseline",
    "Kitchen Garden",
    "Waste Management",
    "Water Conservation",
    "Climate",
    "Life Style & Choices",
    "Endline",
    "Modlues Completed",
    "Attendance %"
FROM clean
WHERE "Student Name" <> ''
