-- Clean and format STP 25-26 Session Plannings
-- Source: STP_25-26.All_Session_Plannings

SELECT
  -- canonical identifier
  CASE WHEN trim("ID"::text) ~ '^\d+$' THEN (trim("ID"::text))::BIGINT ELSE NULL END AS "ID",

  -- Academic Year (expecting format YYYY-YYYY)
  CASE WHEN trim("Academic_Year"::text) ~ '^\d{4}-\d{4}$' THEN trim("Academic_Year"::text) ELSE NULL END AS "Academic Year",

  -- Trainer name cleaned
  NULLIF(initcap(trim("Trainer_Name"::text)), '') AS "Trainer Name",

  -- raw/text fallback of session month for cases we couldn't parse
  NULLIF(trim("Session_Month"::text), '') AS "Session Month",

  -- class info
  NULLIF(trim("Class"::text), '') AS Class,
  NULLIF(trim("Class_Division"::text), '') AS "Class Division",

  -- intervention name (correcting common typos and normalizing)
  NULLIF(initcap(trim("Intervension_Name"::text)), '') AS "Intervention name",
  -- school id/name
  NULLIF(initcap(trim("School_Name"::text)), '') AS "School Name",

  -- session details
  CASE WHEN trim("Session_Planned"::text) ~ '^\d+$' THEN (trim("Session_Planned"::text))::INT ELSE NULL END AS "Session Planned",
  NULLIF(trim("Experiment_Planned"::text), '') AS "Experiment Planned",
  -- donor
  NULLIF(initcap(trim("Donor"::text)), '') AS Donor,

  -- added_Time -> timestamp
  CASE
    WHEN trim("Added_Time"::text) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$' THEN (trim("Added_Time"::text))::timestamp
    WHEN trim("Added_Time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{2}:\d{2}:\d{2}$' THEN to_timestamp(trim("Added_Time"::text), 'DD/MM/YYYY HH24:MI:SS')
    WHEN trim("Added_Time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(trim("Added_Time"::text), 'DD/MM/YYYY')::timestamp
    ELSE NULL
  END AS "Added Time",

  -- source raw (keep full raw row as jsonb, avoid selecting * which can duplicate named columns)
  to_jsonb(src) AS raw_record

FROM {{ source('STP_25-26', 'All_Session_Plannings') }} AS src
WHERE "ID" IS NOT NULL
