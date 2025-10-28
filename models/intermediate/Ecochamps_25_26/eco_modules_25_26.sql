WITH base AS (

    SELECT
        "Student Unique ID",
        "Chapter",
        "School",
        "Grade",
        "School ID",
        "Student Name",
        'Baseline' AS module,
        CASE 
            WHEN LEFT("Baseline", 1) = 'P' THEN 'P'
            WHEN LEFT("Baseline", 1) = 'A' THEN 'A'
            ELSE NULL
        END AS attendance,
        NULLIF(regexp_replace("Baseline", '.*\(([^)]+)\)', '\1'), "Baseline") AS raw_date
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL

    SELECT
        "Student Unique ID",
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
        NULLIF(regexp_replace("Kitchen Garden", '.*\(([^)]+)\)', '\1'), "Kitchen Garden") AS raw_date
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL

    SELECT
        "Student Unique ID",
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
        NULLIF(regexp_replace("Waste Management", '.*\(([^)]+)\)', '\1'), "Waste Management") AS raw_date
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL

    SELECT
        "Student Unique ID",
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
        NULLIF(regexp_replace("Water Conservation", '.*\(([^)]+)\)', '\1'), "Water Conservation") AS raw_date
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL

    SELECT
        "Student Unique ID",
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
        NULLIF(regexp_replace("Climate", '.*\(([^)]+)\)', '\1'), "Climate") AS raw_date
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL

    SELECT
        "Student Unique ID",
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
        NULLIF(regexp_replace("Life Style & Choices", '.*\(([^)]+)\)', '\1'), "Life Style & Choices") AS raw_date
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL

    SELECT
        "Student Unique ID",
        "Chapter",
        "School",
        "Grade",
        "School ID",
        "Student Name",
        'Endline' AS module,
        CASE 
            WHEN LEFT("Endline", 1) = 'P' THEN 'P'
            WHEN LEFT("Endline", 1) = 'A' THEN 'A'
            ELSE NULL
        END AS attendance,
        NULLIF(regexp_replace("Endline", '.*\(([^)]+)\)', '\1'), "Endline") AS raw_date
    FROM {{ ref('eco_student25_26_stg') }}

)

SELECT
    "Student Unique ID",
    "Chapter",
    "School",
    "Grade",
    "School ID",
    "Student Name",
    module,
    attendance,
    CASE
        WHEN raw_date IS NULL THEN NULL
        WHEN raw_date ~ '/' THEN TO_DATE(raw_date, 'MM/DD/YY')
        ELSE TO_DATE(raw_date, 'Mon DD YYYY')
    END AS date
FROM base
