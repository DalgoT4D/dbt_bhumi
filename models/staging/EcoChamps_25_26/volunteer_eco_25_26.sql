WITH center_raw AS (
    SELECT DISTINCT
        COALESCE(BTRIM("Chapter"::TEXT), '') AS "Chapter",
        COALESCE(BTRIM("Volunteer_Name"::TEXT), '') AS "Volunteer Name",
        COALESCE(BTRIM("Volunteers_Mapped__School_Shelter_Name_"::TEXT), '') AS "Volunteers Mapped School",
        COALESCE(BTRIM("Volunteer_Hours"::TEXT), '') AS "Volunteer Hours",
        COALESCE(BTRIM("Total_Sessions_Attended"::TEXT), '') AS "Total Sessions Attended",
        COALESCE(BTRIM("Attendance_Percentage"::TEXT), '') AS "Attendance Percentage"
    FROM {{ source('ecochamps25_26', 'Center_Coordiantors') }}
),

clean AS (
    SELECT
        -- text fields (safe varchar)
        NULLIF(INITCAP("Chapter"), '')::VARCHAR(255) AS "Chapter",
        NULLIF(INITCAP("Volunteer Name"), '')::VARCHAR(255) AS "Volunteer Name",
        NULLIF(INITCAP("Volunteers Mapped School"), '')::VARCHAR(255) AS "Volunteers Mapped School",

        -- volunteer hours: numeric(10,2); accepts "12", "12.5", "12 hrs"
        CASE
            WHEN "Volunteer Hours" ~ '^[0-9]+(\.[0-9]+)?(\s*hrs?)?$'
                THEN regexp_replace("Volunteer Hours", '\s*hrs?$', '', 'i')::NUMERIC(10,2)
            ELSE NULL
        END AS "Volunteer Hours",

        -- total sessions: integer
        CASE
            WHEN "Total Sessions Attended" ~ '^\d+$'
                THEN "Total Sessions Attended"::INT
            ELSE NULL
        END AS "Total Sessions Attended",

        -- attendance %: numeric(5,2) (strips % if present)
        CASE
            WHEN "Attendance Percentage" ~ '^[0-9]+(\.[0-9]+)?%?$'
                THEN REPLACE("Attendance Percentage", '%', '')::NUMERIC(5,2)
            ELSE NULL
        END AS "Attendance Percentage"

    FROM center_raw
)

SELECT
    "Chapter",
    "Volunteer Name",
    "Volunteers Mapped School",
    "Volunteer Hours",
    "Total Sessions Attended",
    "Attendance Percentage"
FROM clean
WHERE "Volunteer Name" <> ''
ORDER BY "Volunteer Name"
