-- Clean and format STP 25-26 Session Plannings
-- Source: STP_25-26.All_Session_Plannings

SELECT
  -- canonical identifier
  CASE WHEN trim("id"::text) ~ '^\d+$' THEN (trim("id"::text))::BIGINT ELSE NULL END AS "ID",

  -- Academic Year (expecting format YYYY-YYYY)
  CASE WHEN trim("academic_year"::text) ~ '^\d{4}-\d{4}$' THEN trim("academic_year"::text) ELSE NULL END AS "Academic year",

  -- Trainer name cleaned
  NULLIF(initcap(trim("trainer_name"::text)), '') AS "Trainer Name",

  -- Session month: try parsing common formats to the first day of the month
  CASE
    WHEN trim("session_month"::text) ~ '^\d{4}-\d{2}$' THEN to_date(trim("session_month"::text) || '-01', 'YYYY-MM-DD')
    WHEN trim("session_month"::text) ~ '^\d{4}-\d{2}-\d{2}$' THEN to_date(trim("session_month"::text), 'YYYY-MM-DD')
    WHEN trim("session_month"::text) ~ '^[A-Za-z]{3}\s+\d{4}$' THEN to_date(trim("session_month"::text), 'Mon YYYY')
    WHEN trim("session_month"::text) ~ '^[A-Za-z]{3,9}\s+\d{4}$' THEN to_date(substring(trim("session_month"::text) from 1 for 3) || ' ' || substring(trim("session_month"::text) from '\\d{4}$'), 'Mon YYYY')
    WHEN trim("session_month"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(trim("session_month"::text), 'DD/MM/YYYY')
    ELSE NULL
  END AS "Session Month",

  -- raw/text fallback of session month for cases we couldn't parse
  NULLIF(trim("session_month"::text), '') AS "Session Month text",

  -- class info
  NULLIF(trim("class"::text), '') AS Class,
  NULLIF(trim("class_division"::text), '') AS "Class Division",

  -- intervention name (correcting common typos and normalizing)
  NULLIF(initcap(trim("intervension_name"::text)), '') AS "Intervention name",

  -- school id/name
  NULLIF(initcap(trim("school_name"::text)), '') AS "School Name",

  -- session details
  NULLIF(trim("session_planned"::text), '') AS "Session Planned",
  NULLIF(trim("experiment_planned"::text), '') AS "Experiment Planned",

  -- donor
  NULLIF(initcap(trim("donor"::text)), '') AS Donor,

  -- added_Time -> timestamp
  CASE
    WHEN trim("added_time"::text) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$' THEN (trim("added_time"::text))::timestamp
    WHEN trim("added_time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{2}:\d{2}:\d{2}$' THEN to_timestamp(trim("added_time"::text), 'DD/MM/YYYY HH24:MI:SS')
    WHEN trim("added_time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(trim("added_time"::text), 'DD/MM/YYYY')::timestamp
    ELSE NULL
  END AS "Added Time",

  -- source raw (keep full raw row as jsonb, avoid selecting * which can duplicate named columns)
  to_jsonb(src) AS raw_record

FROM {{ source('STP_25-26', 'All_Session_Plannings') }} AS src
WHERE ID IS NOT NULL
