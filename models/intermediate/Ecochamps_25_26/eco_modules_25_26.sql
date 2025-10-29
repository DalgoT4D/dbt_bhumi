WITH base AS (
    SELECT
        "Roll No",
        "Chapter",
        "School",
        "Grade",
        "School ID",
        "Student Name",
        'Kitchen Garden' AS module,
        TRIM(SPLIT_PART("Kitchen Garden", '(', 1)) AS raw_value,
        TRIM(REGEXP_REPLACE("Kitchen Garden", '.*\(\s*([^)]+)\s*\).*', '\1')) AS raw_date
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL
    SELECT
        "Roll No", "Chapter", "School", "Grade", "School ID", "Student Name",
        'Waste Management',
        TRIM(SPLIT_PART("Waste Management", '(', 1)),
        TRIM(REGEXP_REPLACE("Waste Management", '.*\(\s*([^)]+)\s*\).*', '\1'))
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL
    SELECT
        "Roll No", "Chapter", "School", "Grade", "School ID", "Student Name",
        'Water Conservation',
        TRIM(SPLIT_PART("Water Conservation", '(', 1)),
        TRIM(REGEXP_REPLACE("Water Conservation", '.*\(\s*([^)]+)\s*\).*', '\1'))
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL
    SELECT
        "Roll No", "Chapter", "School", "Grade", "School ID", "Student Name",
        'Climate',
        TRIM(SPLIT_PART("Climate", '(', 1)),
        TRIM(REGEXP_REPLACE("Climate", '.*\(\s*([^)]+)\s*\).*', '\1'))
    FROM {{ ref('eco_student25_26_stg') }}

    UNION ALL
    SELECT
        "Roll No", "Chapter", "School", "Grade", "School ID", "Student Name",
        'Life Style & Choices',
        TRIM(SPLIT_PART("Life Style & Choices", '(', 1)),
        TRIM(REGEXP_REPLACE("Life Style & Choices", '.*\(\s*([^)]+)\s*\).*', '\1'))
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
    CASE 
        WHEN raw_value ILIKE 'P%' THEN 'P'
        WHEN raw_value ILIKE 'A%' THEN 'A'
        ELSE NULL
    END AS attendance,
    CASE
        WHEN raw_date ~ '^[A-Za-z]+[[:space:]]+[0-9]{1,2}[[:space:]]+[0-9]{4}$' 
            THEN TO_DATE(raw_date, 'Mon DD YYYY')
        WHEN raw_date ~ '^[0-9]{1,2}[[:space:]]+[A-Za-z]+[[:space:]]+[0-9]{4}$' 
            THEN TO_DATE(raw_date, 'DD Mon YYYY')
        ELSE NULL
    END AS date
FROM base
WHERE TRIM(raw_value) <> ''
ORDER BY "School ID", "Grade", "Roll No", module
