WITH data_tracker_clean AS (
    SELECT DISTINCT
        COALESCE(BTRIM("School"::TEXT), '') AS "School",
        COALESCE(BTRIM("Grade"::TEXT), '') AS "Grade",
        COALESCE(BTRIM("Donor_Mapped"::TEXT), '') AS "Donor Mapped"
    FROM {{ source('ecochamps25_26', 'Data_Tracker') }}
    WHERE "Donor_Mapped" IS NOT NULL
),

clean AS (
    SELECT
        COALESCE(INITCAP(BTRIM(ssd."Chapter"::TEXT)), '') AS "Chapter",
        COALESCE(INITCAP(BTRIM(ssd."School"::TEXT)), '') AS "School",
        COALESCE(BTRIM(ssd."Grade"::TEXT), '') AS "Grade",
        CASE WHEN BTRIM(ssd."School_ID"::TEXT) ~ '^\d+$' THEN (ssd."School_ID"::TEXT)::BIGINT END AS "School ID",
        CASE WHEN BTRIM(ssd."Roll_No_"::TEXT) ~ '^\d+$' THEN (ssd."Roll_No_"::TEXT)::BIGINT END AS "Roll No",
        COALESCE(INITCAP(BTRIM(ssd."_Student_Name_"::TEXT)), '') AS "Student Name",
        COALESCE(INITCAP(BTRIM(ssd."Center_Coordiantor__1_"::TEXT)), '') AS "Center Coordiantor (1)",
        COALESCE(INITCAP(BTRIM(ssd."Center_Coordianator__2_"::TEXT)), '') AS "Center Coordianator (2)",
        COALESCE(BTRIM(ssd."Student_Status"::TEXT), '') AS "Student Status",

        dt."Donor Mapped",

        -- Baseline and Endline scores
        CASE 
            WHEN BTRIM(ssd."Baseline_Score"::TEXT) ~ '^[0-9]+(\.[0-9]+)?$' 
                THEN (ssd."Baseline_Score"::TEXT)::NUMERIC 
            WHEN UPPER(BTRIM(ssd."Baseline_Score"::TEXT)) = 'A' 
                THEN 0 
        END AS "Baseline Score",

        CASE 
            WHEN BTRIM(ssd."Endline_Sore"::TEXT) ~ '^[0-9]+(\.[0-9]+)?$' 
                THEN (ssd."Endline_Sore"::TEXT)::NUMERIC 
            WHEN UPPER(BTRIM(ssd."Endline_Sore"::TEXT)) = 'A' 
                THEN 0 
        END AS "Endline Score",

        COALESCE(BTRIM(ssd."Baseline"::TEXT), '') AS "Baseline",
        COALESCE(BTRIM(ssd."Kitchen_Garden"::TEXT), '') AS "Kitchen Garden",
        COALESCE(BTRIM(ssd."Waste_Management"::TEXT), '') AS "Waste Management",
        COALESCE(BTRIM(ssd."Water_Conservation"::TEXT), '') AS "Water Conservation",
        COALESCE(BTRIM(ssd."Climate"::TEXT), '') AS "Climate",
        COALESCE(BTRIM(ssd."Life_Style___Choices"::TEXT), '') AS "Life Style & Choices",
        COALESCE(BTRIM(ssd."Endline"::TEXT), '') AS "Endline",

        CASE WHEN BTRIM(ssd."Modlues_Completed"::TEXT) ~ '^\d+$' THEN (ssd."Modlues_Completed"::TEXT)::INT ELSE 0 END AS "Modlues Completed",
        CASE WHEN BTRIM(ssd."Attendance__"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE(ssd."Attendance__", '%', '')::NUMERIC END AS "Attendance %"

    FROM {{ source('ecochamps25_26', 'Student___Session_Data') }} AS ssd
    LEFT JOIN data_tracker_clean AS dt
        ON
            INITCAP(BTRIM(ssd."School"::TEXT)) = INITCAP(BTRIM(dt."School"::TEXT))
            AND BTRIM(ssd."Grade"::TEXT) = BTRIM(dt."Grade"::TEXT)
)

SELECT DISTINCT ON ("School ID", "Roll No", "Student Name")
    "Roll No",
    "Chapter",
    "School",
    "Grade",
    "School ID",
    "Student Name",
    "Center Coordiantor (1)",
    "Center Coordianator (2)",
    "Student Status",
    "Donor Mapped",
    "Baseline Score",
    "Endline Score",
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
WHERE "Roll No" IS NOT NULL
ORDER BY "School ID", "Roll No", "Student Name"
