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

        -- Kitchen Garden
        CASE 
            WHEN "Kitchen Garden" ~ '^P' THEN 'Present'
            WHEN "Kitchen Garden" ~ '^A' THEN 'Absent'
            ELSE NULL 
        END AS kitchen_garden_attendance,
        CASE
            WHEN "Kitchen Garden" ~ '^[PA]\s*\('
            THEN NULLIF(TRIM(REGEXP_REPLACE("Kitchen Garden", '^[PA]\s*\((.*?)\)', '\1')), '')
            ELSE NULL
        END AS kitchen_garden_date,

        -- Waste Management
        CASE 
            WHEN "Waste Management" ~ '^P' THEN 'Present'
            WHEN "Waste Management" ~ '^A' THEN 'Absent'
            ELSE NULL 
        END AS waste_management_attendance,
        CASE
            WHEN "Waste Management" ~ '^[PA]\s*\('
            THEN NULLIF(TRIM(REGEXP_REPLACE("Waste Management", '^[PA]\s*\((.*?)\)', '\1')), '')
            ELSE NULL
        END AS waste_management_date,

        -- Water Conservation
        CASE 
            WHEN "Water Conservation" ~ '^P' THEN 'Present'
            WHEN "Water Conservation" ~ '^A' THEN 'Absent'
            ELSE NULL 
        END AS water_conservation_attendance,
        CASE
            WHEN "Water Conservation" ~ '^[PA]\s*\('
            THEN NULLIF(TRIM(REGEXP_REPLACE("Water Conservation", '^[PA]\s*\((.*?)\)', '\1')), '')
            ELSE NULL
        END AS water_conservation_date,

        -- Climate
        CASE 
            WHEN "Climate" ~ '^P' THEN 'Present'
            WHEN "Climate" ~ '^A' THEN 'Absent'
            ELSE NULL 
        END AS climate_attendance,
        CASE
            WHEN "Climate" ~ '^[PA]\s*\('
            THEN NULLIF(TRIM(REGEXP_REPLACE("Climate", '^[PA]\s*\((.*?)\)', '\1')), '')
            ELSE NULL
        END AS climate_date,

        -- Life Style & Choices
        CASE 
            WHEN "Life Style & Choices" ~ '^P' THEN 'Present'
            WHEN "Life Style & Choices" ~ '^A' THEN 'Absent'
            ELSE NULL 
        END AS lifestyle_choices_attendance,
        CASE
            WHEN "Life Style & Choices" ~ '^[PA]\s*\('
            THEN NULLIF(TRIM(REGEXP_REPLACE("Life Style & Choices", '^[PA]\s*\((.*?)\)', '\1')), '')
            ELSE NULL
        END AS lifestyle_choices_date

    FROM {{ ref('eco_student25_26_stg') }}
),

parsed_dates AS (
    SELECT 
        *,
        -- Kitchen Garden
        CASE 
            WHEN kitchen_garden_date IS NOT NULL THEN
                CASE 
                    WHEN kitchen_garden_date ~ '^\d{1,2}\s+[A-Za-z]{3}\s+\d{4}$' THEN TO_DATE(kitchen_garden_date, 'DD Mon YYYY')
                    WHEN kitchen_garden_date ~ '^[A-Za-z]{3}\s+\d{1,2}\s+\d{4}$' THEN TO_DATE(kitchen_garden_date, 'Mon DD YYYY')
                    WHEN kitchen_garden_date ~ '^\d{1,2}/\d{1,2}/\d{2}$' THEN TO_DATE(kitchen_garden_date, 'DD/MM/YY')
                    WHEN kitchen_garden_date ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(kitchen_garden_date, 'DD/MM/YYYY')
                    ELSE NULL
                END
            ELSE NULL 
        END AS kitchen_garden_date_parsed,

        -- Waste Management
        CASE 
            WHEN waste_management_date IS NOT NULL THEN
                CASE 
                    WHEN waste_management_date ~ '^\d{1,2}\s+[A-Za-z]{3}\s+\d{4}$' THEN TO_DATE(waste_management_date, 'DD Mon YYYY')
                    WHEN waste_management_date ~ '^[A-Za-z]{3}\s+\d{1,2}\s+\d{4}$' THEN TO_DATE(waste_management_date, 'Mon DD YYYY')
                    WHEN waste_management_date ~ '^\d{1,2}/\d{1,2}/\d{2}$' THEN TO_DATE(waste_management_date, 'DD/MM/YY')
                    WHEN waste_management_date ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(waste_management_date, 'DD/MM/YYYY')
                    ELSE NULL
                END
            ELSE NULL
        END AS waste_management_date_parsed,

        -- Water Conservation
        CASE 
            WHEN water_conservation_date IS NOT NULL THEN
                CASE 
                    WHEN water_conservation_date ~ '^\d{1,2}\s+[A-Za-z]{3}\s+\d{4}$' THEN TO_DATE(water_conservation_date, 'DD Mon YYYY')
                    WHEN water_conservation_date ~ '^[A-Za-z]{3}\s+\d{1,2}\s+\d{4}$' THEN TO_DATE(water_conservation_date, 'Mon DD YYYY')
                    WHEN water_conservation_date ~ '^\d{1,2}/\d{1,2}/\d{2}$' THEN TO_DATE(water_conservation_date, 'DD/MM/YY')
                    WHEN water_conservation_date ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(water_conservation_date, 'DD/MM/YYYY')
                    ELSE NULL
                END
            ELSE NULL
        END AS water_conservation_date_parsed,

        -- Climate
        CASE 
            WHEN climate_date IS NOT NULL THEN
                CASE 
                    WHEN climate_date ~ '^\d{1,2}\s+[A-Za-z]{3}\s+\d{4}$' THEN TO_DATE(climate_date, 'DD Mon YYYY')
                    WHEN climate_date ~ '^[A-Za-z]{3}\s+\d{1,2}\s+\d{4}$' THEN TO_DATE(climate_date, 'Mon DD YYYY')
                    WHEN climate_date ~ '^\d{1,2}/\d{1,2}/\d{2}$' THEN TO_DATE(climate_date, 'DD/MM/YY')
                    WHEN climate_date ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(climate_date, 'DD/MM/YYYY')
                    ELSE NULL
                END
            ELSE NULL
        END AS climate_date_parsed,

        -- Life Style & Choices
        CASE 
            WHEN lifestyle_choices_date IS NOT NULL THEN
                CASE 
                    WHEN lifestyle_choices_date ~ '^\d{1,2}\s+[A-Za-z]{3}\s+\d{4}$' THEN TO_DATE(lifestyle_choices_date, 'DD Mon YYYY')
                    WHEN lifestyle_choices_date ~ '^[A-Za-z]{3}\s+\d{1,2}\s+\d{4}$' THEN TO_DATE(lifestyle_choices_date, 'Mon DD YYYY')
                    WHEN lifestyle_choices_date ~ '^\d{1,2}/\d{1,2}/\d{2}$' THEN TO_DATE(lifestyle_choices_date, 'DD/MM/YY')
                    WHEN lifestyle_choices_date ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(lifestyle_choices_date, 'DD/MM/YYYY')
                    ELSE NULL
                END
            ELSE NULL
        END AS lifestyle_choices_date_parsed

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
    kitchen_garden_attendance,
    kitchen_garden_date_parsed   AS kitchen_garden_date,
    waste_management_attendance,
    waste_management_date_parsed AS waste_management_date,
    water_conservation_attendance,
    water_conservation_date_parsed AS water_conservation_date,
    climate_attendance,
    climate_date_parsed          AS climate_date,
    lifestyle_choices_attendance,
    lifestyle_choices_date_parsed AS lifestyle_choices_date,

    -- number of modules completed: sum of module attendance flags (1 when attendance present/non-empty), default 0 if null
    COALESCE(
        (
            (CASE WHEN kitchen_garden_attendance IS NOT NULL AND TRIM(kitchen_garden_attendance) <> '' THEN 1 ELSE 0 END)
            + (CASE WHEN waste_management_attendance IS NOT NULL AND TRIM(waste_management_attendance) <> '' THEN 1 ELSE 0 END)
            + (CASE WHEN water_conservation_attendance IS NOT NULL AND TRIM(water_conservation_attendance) <> '' THEN 1 ELSE 0 END)
            + (CASE WHEN climate_attendance IS NOT NULL AND TRIM(climate_attendance) <> '' THEN 1 ELSE 0 END)
            + (CASE WHEN lifestyle_choices_attendance IS NOT NULL AND TRIM(lifestyle_choices_attendance) <> '' THEN 1 ELSE 0 END)
        ),
        0
    ) AS "Modules completed",

    -- Modules attendance %:
    CASE
        WHEN (
            (CASE WHEN kitchen_garden_attendance IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN waste_management_attendance IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN water_conservation_attendance IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN climate_attendance IS NOT NULL THEN 1 ELSE 0 END) +
            (CASE WHEN lifestyle_choices_attendance IS NOT NULL THEN 1 ELSE 0 END)
        ) = 0 THEN NULL
        ELSE
            ROUND(
                (
                    (CASE WHEN kitchen_garden_attendance = 'Present' THEN 1 ELSE 0 END) +
                    (CASE WHEN waste_management_attendance = 'Present' THEN 1 ELSE 0 END) +
                    (CASE WHEN water_conservation_attendance = 'Present' THEN 1 ELSE 0 END) +
                    (CASE WHEN climate_attendance = 'Present' THEN 1 ELSE 0 END) +
                    (CASE WHEN lifestyle_choices_attendance = 'Present' THEN 1 ELSE 0 END)
                )::NUMERIC
                /
                (
                    (CASE WHEN kitchen_garden_attendance IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN waste_management_attendance IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN water_conservation_attendance IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN climate_attendance IS NOT NULL THEN 1 ELSE 0 END) +
                    (CASE WHEN lifestyle_choices_attendance IS NOT NULL THEN 1 ELSE 0 END)
                ) * 100
            , 2)
    END AS "Modules Attendance %"

FROM parsed_dates
