-- Clean and format STP 25-26 Session Plannings
-- Source: STP_25-26.All_Session_Plannings

SELECT
  -- canonical identifier
  CASE WHEN trim("ID"::text) ~ '^\d+$' THEN (trim("ID"::text))::BIGINT ELSE NULL END AS ID,

  -- Academic Year (expecting format YYYY-YYYY)
  CASE WHEN trim("Academic_Year"::text) ~ '^\d{4}-\d{4}$' THEN trim("Academic_Year"::text) ELSE NULL END AS "Academic year",

  -- Trainer name cleaned
  NULLIF(initcap(trim("Trainer_Name"::text)), '') AS "Trainer Name",

  -- Session month: try parsing common formats to the first day of the month
  CASE
    WHEN trim("Session_Month"::text) ~ '^\d{4}-\d{2}$' THEN to_date(trim("Session_Month"::text) || '-01', 'YYYY-MM-DD')
    WHEN trim("Session_Month"::text) ~ '^\d{4}-\d{2}-\d{2}$' THEN to_date(trim("Session_Month"::text), 'YYYY-MM-DD')
    WHEN trim("Session_Month"::text) ~ '^[A-Za-z]{3}\s+\d{4}$' THEN to_date(trim("Session_Month"::text), 'Mon YYYY')
    WHEN trim("Session_Month"::text) ~ '^[A-Za-z]{3,9}\s+\d{4}$' THEN to_date(substring(trim("Session_Month"::text) from 1 for 3) || ' ' || substring(trim("Session_Month"::text) from '\\d{4}$'), 'Mon YYYY')
    WHEN trim("Session_Month"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(trim("Session_Month"::text), 'DD/MM/YYYY')
    ELSE NULL
  END AS "Session Month",

  -- raw/text fallback of session month for cases we couldn't parse
  NULLIF(trim("Session_Month"::text), '') AS "Session Month text",

  -- class info
  NULLIF(trim("Class"::text), '') AS Class,
  NULLIF(trim("Class_Division"::text), '') AS "Class Division",

  -- intervention name (correcting common typos and normalizing)
  NULLIF(initcap(trim("Intervension_Name"::text)), '') AS "Intervention name",

  -- school id/name
  NULLIF(initcap(trim("School_Name"::text)), '') AS "School Name",

  -- session details
  NULLIF(trim("Session_Planned"::text), '') AS "Session Planned",
  NULLIF(trim("Experiment_Planned"::text), '') AS "Experiment Planned",

  -- donor
  NULLIF(initcap(trim("Donor"::text)), '') AS Donor,

  -- Added_Time -> timestamp
  CASE
    WHEN trim("Added_Time"::text) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$' THEN (trim("Added_Time"::text))::timestamp
    WHEN trim("Added_Time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{2}:\d{2}:\d{2}$' THEN to_timestamp(trim("Added_Time"::text), 'DD/MM/YYYY HH24:MI:SS')
    WHEN trim("Added_Time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(trim("Added_Time"::text), 'DD/MM/YYYY')::timestamp
    ELSE NULL
  END AS "Added Time",

  -- source raw
  *

FROM {{ source('STP_25-26', 'All_Session_Plannings') }}

