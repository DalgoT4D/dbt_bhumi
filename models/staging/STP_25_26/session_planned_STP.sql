-- Clean and format STP 25-26 Session Plannings
-- Source: STP_25-26.All_Session_Plannings

SELECT
  -- canonical identifier
  CASE WHEN trim("ID"::text) ~ '^\d+$' THEN (trim("ID"::text))::BIGINT ELSE NULL END AS id,

  -- Academic Year (expecting format YYYY-YYYY)
  CASE WHEN trim("Academic Year"::text) ~ '^\d{4}-\d{4}$' THEN trim("Academic Year"::text) ELSE NULL END AS academic_year,

  -- Trainer name cleaned
  NULLIF(initcap(trim("Trainer Name"::text)), '') AS trainer_name,

  -- Session month: try parsing common formats to the first day of the month
  CASE
    WHEN trim("Session Month"::text) ~ '^\d{4}-\d{2}$' THEN to_date(trim("Session Month"::text) || '-01', 'YYYY-MM-DD')
    WHEN trim("Session Month"::text) ~ '^\d{4}-\d{2}-\d{2}$' THEN to_date(trim("Session Month"::text), 'YYYY-MM-DD')
    WHEN trim("Session Month"::text) ~ '^[A-Za-z]{3}\s+\d{4}$' THEN to_date(trim("Session Month"::text), 'Mon YYYY')
    WHEN trim("Session Month"::text) ~ '^[A-Za-z]{3,9}\s+\d{4}$' THEN to_date(substring(trim("Session Month"::text) from 1 for 3) || ' ' || substring(trim("Session Month"::text) from '\\d{4}$'), 'Mon YYYY')
    WHEN trim("Session Month"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(trim("Session Month"::text), 'DD/MM/YYYY')
    ELSE NULL
  END AS session_month,

  -- raw/text fallback of session month for cases we couldn't parse
  NULLIF(trim("Session Month"::text), '') AS session_month_text,

  -- class info
  NULLIF(trim("Class"::text), '') AS class,
  NULLIF(trim("Class Division"::text), '') AS class_division,

  -- intervention name (correcting common typos and normalizing)
  NULLIF(initcap(trim("Intervension Name"::text)), '') AS intervention_name,

  -- school id/name
  NULLIF(initcap(trim("School Name"::text)), '') AS school_name,

  -- session details
  NULLIF(trim("Session Planned"::text), '') AS session_planned,
  NULLIF(trim("Experiment Planned"::text), '') AS experiment_planned,

  -- donor
  NULLIF(initcap(trim("Donor"::text)), '') AS donor,

  -- added time -> timestamp
  CASE
    WHEN trim("Added Time"::text) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$' THEN (trim("Added Time"::text))::timestamp
    WHEN trim("Added Time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{2}:\d{2}:\d{2}$' THEN to_timestamp(trim("Added Time"::text), 'DD/MM/YYYY HH24:MI:SS')
    WHEN trim("Added Time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(trim("Added Time"::text), 'DD/MM/YYYY')::timestamp
    ELSE NULL
  END AS added_time,

  -- source raw
  *

FROM {{ source('STP_25-26', 'All_Session_Plannings') }}

