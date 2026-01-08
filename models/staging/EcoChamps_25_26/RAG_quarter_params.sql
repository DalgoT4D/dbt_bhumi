WITH RAG_QUARTER_PARAMS_RAW AS (
    SELECT DISTINCT
        COALESCE(BTRIM("Year"::TEXT), '') AS "Year",
        COALESCE(BTRIM("Quarter"::TEXT), '') AS "Quarter",
        COALESCE(BTRIM("Quarter_Start_Date"::TEXT), '') AS "Quarter Start Date",
        COALESCE(BTRIM("Quarter_End_Date"::TEXT), '') AS "Quarter End Date",
        COALESCE(BTRIM("Target_Modules"::TEXT), '') AS "Target Modules",
        COALESCE(BTRIM("Green_percent_comp"::TEXT), '') AS "Green Percent Comp",
        COALESCE(BTRIM("Amber_percent_comp"::TEXT), '') AS "Amber Percent Comp",
        COALESCE(BTRIM("Red_percent_comp"::TEXT), '') AS "Red Percent Comp"
    FROM {{ source('ecochamps25_26', 'RAG_Quarter_Params') }}
),

CLEAN AS (
    SELECT
        -- text fields (safe varchar)
        NULLIF(INITCAP("Quarter"), '')::VARCHAR(255) AS "Quarter",

        -- Quarter Start Date & End Date: only accept dd/mm/yyyy
        CASE
            WHEN
                BTRIM(REGEXP_REPLACE("Quarter Start Date", '(st|nd|rd|th)', '', 'i')) 
                ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/[0-9]{4}$'
                THEN TO_DATE(BTRIM(REGEXP_REPLACE("Quarter Start Date", '(st|nd|rd|th)', '', 'i')), 'DD/MM/YYYY')
        END AS "Quarter Start Date",
        CASE
            WHEN
                BTRIM(REGEXP_REPLACE("Quarter End Date", '(st|nd|rd|th)', '', 'i')) 
                ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/[0-9]{4}$'
                THEN TO_DATE(BTRIM(REGEXP_REPLACE("Quarter End Date", '(st|nd|rd|th)', '', 'i')), 'DD/MM/YYYY')
        END AS "Quarter End Date",

        -- year, no. target modules: integer
        CASE
            WHEN "Year" ~ '^\d+$'
                THEN "Year"::INT
        END AS "Year",
        CASE
            WHEN "Target Modules" ~ '^\d+$'
                THEN "Target Modules"::INT
        END AS "Target Modules",

        -- R,A,G percent comp %: numeric(5,2) (strips % if present)
        CASE
            WHEN "Green Percent Comp" ~ '^[0-9]+(\.[0-9]+)?%?$'
                THEN REPLACE("Green Percent Comp", '%', '')::NUMERIC(5,2)
        END AS "Green Percent Comp",
        CASE
            WHEN "Amber Percent Comp" ~ '^[0-9]+(\.[0-9]+)?%?$'
                THEN REPLACE("Amber Percent Comp", '%', '')::NUMERIC(5,2)
        END AS "Amber Percent Comp",
        CASE
            WHEN "Red Percent Comp" ~ '^[0-9]+(\.[0-9]+)?%?$'
                THEN REPLACE("Red Percent Comp", '%', '')::NUMERIC(5,2)
        END AS "Red Percent Comp"

    FROM RAG_QUARTER_PARAMS_RAW
)

SELECT
    "Year",
    "Quarter",
    "Quarter Start Date",
    "Quarter End Date",
    "Target Modules",
    "Green Percent Comp",
    "Amber Percent Comp",
    "Red Percent Comp"
FROM CLEAN
WHERE "Year" IS NOT NULL AND "Quarter" IS NOT NULL
ORDER BY 1,2
