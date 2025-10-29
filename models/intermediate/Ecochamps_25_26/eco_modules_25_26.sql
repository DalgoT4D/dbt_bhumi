WITH base AS (
    SELECT
        "Roll No",
        "Chapter",
        "School",
        "Grade",
        "School ID",
        "Student Name",
        'Kitchen Garden' AS module,
        CASE 
            WHEN LEFT("Kitchen Garden", 1) = 'P' THEN 'P'
            WHEN LEFT("Kitchen Garden", 1) = 'A' THEN 'A'
            ELSE NULL
        END AS attendance,
        CASE 
            WHEN "Kitchen Garden" ~ '\\(\\w{3} \\d{1,2} \\d{4}\\)' THEN TO_DATE(regexp_replace("Kitchen Garden", '.*\\(([^)]+)\\)', '\\1'), 'Mon DD YYYY')
            ELSE NULL
        END AS date
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL

    SELECT
        "Roll No",
        "Chapter",
        "School",
        "Grade",
        "School ID",
        "Student Name",
        'Waste Management' AS module,
        CASE 
            WHEN LEFT("Waste Management", 1) = 'P' THEN 'P'
            WHEN LEFT("Waste Management", 1) = 'A' THEN 'A'
            ELSE NULL
        END AS attendance,
        CASE 
            WHEN "Waste Management" ~ '\\(\\w{3} \\d{1,2} \\d{4}\\)' THEN TO_DATE(regexp_replace("Waste Management", '.*\\(([^)]+)\\)', '\\1'), 'Mon DD YYYY')
            ELSE NULL
        END AS date
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL

    SELECT
        "Roll No",
        "Chapter",
        "School",
        "Grade",
        "School ID",
        "Student Name",
        'Water Conservation' AS module,
        CASE 
            WHEN LEFT("Water Conservation", 1) = 'P' THEN 'P'
            WHEN LEFT("Water Conservation", 1) = 'A' THEN 'A'
            ELSE NULL
        END AS attendance,
        CASE 
            WHEN "Water Conservation" ~ '\\(\\w{3} \\d{1,2} \\d{4}\\)' THEN TO_DATE(regexp_replace("Water Conservation", '.*\\(([^)]+)\\)', '\\1'), 'Mon DD YYYY')
            ELSE NULL
        END AS date
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL

    SELECT
        "Roll No",
        "Chapter",
        "School",
        "Grade",
        "School ID",
        "Student Name",
        'Climate' AS module,
        CASE 
            WHEN LEFT("Climate", 1) = 'P' THEN 'P'
            WHEN LEFT("Climate", 1) = 'A' THEN 'A'
            ELSE NULL
        END AS attendance,
        CASE 
            WHEN "Climate" ~ '\\(\\w{3} \\d{1,2} \\d{4}\\)' THEN TO_DATE(regexp_replace("Climate", '.*\\(([^)]+)\\)', '\\1'), 'Mon DD YYYY')
            ELSE NULL
        END AS date
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL

    SELECT
        "Roll No",
        "Chapter",
        "School",
        "Grade",
        "School ID",
        "Student Name",
        'Life Style & Choices' AS module,
        CASE 
            WHEN LEFT("Life Style & Choices", 1) = 'P' THEN 'P'
            WHEN LEFT("Life Style & Choices", 1) = 'A' THEN 'A'
            ELSE NULL
        END AS attendance,
        CASE 
            WHEN "Life Style & Choices" ~ '\\(\\w{3} \\d{1,2} \\d{4}\\)' THEN TO_DATE(regexp_replace("Life Style & Choices", '.*\\(([^)]+)\\)', '\\1'), 'Mon DD YYYY')
            ELSE NULL
        END AS date
    FROM {{ ref('eco_student25_26_stg') }}
)

SELECT
    "Roll No",
    "Chapter",
    "School",
    "Grade",
    "School ID",
    "Student Name",
    module,
    attendance,
    date
FROM bas