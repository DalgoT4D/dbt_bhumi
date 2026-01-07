WITH RAG_Quarter_Params_raw AS (
    SELECT DISTINCT
        COALESCE(BTRIM("year"::TEXT), '') AS "Year",
        COALESCE(BTRIM("quarter"::TEXT), '') AS "Quarter",
        COALESCE(BTRIM("quarter_start_date"::TEXT), '') AS "Quarter Start Date",
        COALESCE(BTRIM("quarter_end_date"::TEXT), '') AS "Quarter End Date",
        COALESCE(BTRIM("target_modules"::TEXT), '') AS "Target Modules",
        COALESCE(BTRIM("green_percent_comp"::TEXT), '') AS "Green Percent Comp",
        COALESCE(BTRIM("amber_percent_comp"::TEXT), '') AS "Amber Percent Comp",
        COALESCE(BTRIM("red_percent_comp"::TEXT), '') AS "Red Percent Comp"
    FROM {{ source('ecochamps25_26', 'RAG_Quarter_Params') }}
),

clean AS (
    SELECT
        -- text fields (safe varchar)
        NULLIF(INITCAP("Quarter"), '')::VARCHAR(255) AS "Quarter",

        -- Quarter Start Date & End Date: only accept dd/mm/yyyy
        CASE
            WHEN BTRIM(REGEXP_REPLACE("Quarter Start Date", '(st|nd|rd|th)', '', 'i')) 
                 ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/[0-9]{4}$'
                THEN to_date(BTRIM(REGEXP_REPLACE("Quarter Start Date", '(st|nd|rd|th)', '', 'i')), 'DD/MM/YYYY')
            ELSE NULL
        END AS "Quarter Start Date",
        CASE
            WHEN BTRIM(REGEXP_REPLACE("Quarter End Date", '(st|nd|rd|th)', '', 'i')) 
                 ~ '^(0?[1-9]|[12][0-9]|3[01])/(0?[1-9]|1[0-2])/[0-9]{4}$'
                THEN to_date(BTRIM(REGEXP_REPLACE("Quarter End Date", '(st|nd|rd|th)', '', 'i')), 'DD/MM/YYYY')
            ELSE NULL
        END AS "Quarter End Date",

        -- year, no. target modules: integer
        CASE
            WHEN "Year" ~ '^\d+$'
                THEN "Year"::INT
            ELSE NULL
        END AS "Year",
        CASE
            WHEN "Target Modules" ~ '^\d+$'
                THEN "Target Modules"::INT
            ELSE NULL
        END AS "Target Modules",

        -- R,A,G percent comp %: numeric(5,2) (strips % if present)
        CASE
            WHEN "Green Percent Comp" ~ '^[0-9]+(\.[0-9]+)?%?$'
                THEN REPLACE("Green Percent Comp", '%', '')::NUMERIC(5,2)
            ELSE NULL
        END AS "Green Percent Comp",
        CASE
            WHEN "Amber Percent Comp" ~ '^[0-9]+(\.[0-9]+)?%?$'
                THEN REPLACE("Amber Percent Comp", '%', '')::NUMERIC(5,2)
            ELSE NULL
        END AS "Amber Percent Comp",
        CASE
            WHEN "Red Percent Comp" ~ '^[0-9]+(\.[0-9]+)?%?$'
                THEN REPLACE("Red Percent Comp", '%', '')::NUMERIC(5,2)
            ELSE NULL
        END AS "Red Percent Comp"

    FROM RAG_Quarter_Params_raw
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
FROM clean
WHERE "Year" IS NOT NULL and "Quarter" IS NOT NULL
ORDER BY 1,2
