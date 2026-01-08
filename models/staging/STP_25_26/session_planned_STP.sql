-- Clean and format STP 25-26 Session Plannings
-- Source: STP_25-26.All_Session_Plannings

SELECT
    -- canonical identifier
    CASE WHEN trim("ID"::text) ~ '^\d+$' THEN (trim("ID"::text))::bigint END AS "ID",

    -- Academic Year (expecting format YYYY-YYYY)
    CASE WHEN trim("Academic_Year"::text) ~ '^\d{4}-\d{4}$' THEN trim("Academic_Year"::text) END AS "Academic Year",

    -- Trainer name cleaned
    nullif(initcap(trim("Trainer_Name"::text)), '') AS "Trainer Name",

    -- raw/text fallback of session month for cases we couldn't parse
    nullif(trim("Session_Month"::text), '') AS "Session Month",

    -- class info
    nullif(trim("Class"::text), '') AS Class,
    nullif(trim("Class_Division"::text), '') AS "Class Division",

    -- intervention name (correcting common typos and normalizing)
    nullif(initcap(trim("Intervension_Name"::text)), '') AS "Intervention name",
    -- school id/name
    nullif(initcap(trim("School_Name"::text)), '') AS "School Name",

    -- session details
    CASE WHEN trim("Session_Planned"::text) ~ '^\d+$' THEN (trim("Session_Planned"::text))::int END AS "Session Planned",
    nullif(trim("Experiment_Planned"::text), '') AS "Experiment Planned",
    -- donor
    nullif(initcap(trim("Donor"::text)), '') AS Donor,

    -- added_Time -> timestamp
    CASE
        WHEN trim("Added_Time"::text) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$' THEN (trim("Added_Time"::text))::timestamp
        WHEN trim("Added_Time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{2}:\d{2}:\d{2}$' THEN to_timestamp(trim("Added_Time"::text), 'DD/MM/YYYY HH24:MI:SS')
        WHEN trim("Added_Time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(trim("Added_Time"::text), 'DD/MM/YYYY')::timestamp
    END AS "Added Time"


FROM {{ source('STP_25-26', 'All_Session_Plannings') }}
WHERE "ID" IS NOT NULL
