-- Cleaned STP 25-26 Session Attendances
-- Source: STP_25-26.All_Session_Attendances

SELECT
  -- canonical identifier
  CASE
    WHEN trim(src."ID"::text) ~ '^\d+$' THEN trim(src."ID"::text)::BIGINT
    ELSE NULL
  END AS "ID",

  -- Was this class combined with another section? -> normalized boolean + raw text
  CASE
    WHEN lower(trim(src."Was_this_class_combined_with_another_section_"::text)) ~ '^(yes|y|true|1)$' THEN TRUE
    WHEN lower(trim(src."Was_this_class_combined_with_another_section_"::text)) ~ '^(no|n|false|0)$' THEN FALSE
    ELSE NULL
  END AS was_class_combined,

  -- Academic Year (expecting format YYYY-YYYY)
  CASE
      WHEN trim(src."Academic_Year"::text) ~ '^\d{4}-\d{4}$' THEN trim(src."Academic_Year"::text)
    ELSE NULL
  END AS academic_year,

  -- School & class info
    NULLIF(initcap(trim(src."School_Name"::text)), '') AS school_name,
    NULLIF(initcap(trim(src."Class_Type"::text)), '') AS class_type,
    NULLIF(initcap(trim(src."Trainer"::text)), '') AS trainer_name,
    NULLIF(trim(src."Class"::text), '') AS class,
    NULLIF(trim(src."Class_Division"::text), '') AS class_division,
  -- Intervention and session metadata
    NULLIF(initcap(trim(src."Intervension_Name"::text)), '') AS intervention_name,
    NULLIF(initcap(trim(src."Session_Type"::text)), '') AS session_type,

  -- session_date: try several common formats, return as date
  CASE
    WHEN trim(src."Session_Date"::text) ~ '^\d{4}-\d{2}-\d{2}$' THEN to_date(trim(src."Session_Date"::text), 'YYYY-MM-DD')
    WHEN trim(src."Session_Date"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(trim(src."Session_Date"::text), 'DD/MM/YYYY')
    WHEN trim(src."Session_Date"::text) ~ '^\d{1,2}-[A-Za-z]{3}-\d{2,4}$' THEN to_date(trim(src."Session_Date"::text), 'DD-Mon-YYYY')
    WHEN trim(src."Session_Date"::text) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$' THEN to_timestamp(trim(src."Session_Date"::text), 'YYYY-MM-DD HH24:MI:SS')::date
    ELSE NULL
  END AS session_date,

  -- Present / Absent / Total -> integers
    CASE WHEN trim(src."Present_Students"::text) ~ '^\d+$' THEN trim(src."Present_Students"::text)::INT ELSE NULL END AS present_students,
    CASE WHEN trim(src."Absent_Students"::text) ~ '^\d+$' THEN trim(src."Absent_Students"::text)::INT ELSE NULL END AS absent_students,
    CASE WHEN trim(src."Total_Student"::text) ~ '^\d+$' THEN trim(src."Total_Student"::text)::INT ELSE NULL END AS total_students,

  --"added_time -> timestamp
  CASE
        WHEN trim(src."Added_Time"::text) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$' THEN (trim(src."Added_Time"::text))::timestamp
        WHEN trim(src."Added_Time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{2}:\d{2}:\d{2}$' THEN to_timestamp(trim(src."Added_Time"::text), 'DD/MM/YYYY HH24:MI:SS')
        WHEN trim(src."Added_Time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(trim(src."Added_Time"::text), 'DD/MM/YYYY')::timestamp
    ELSE NULL
  END AS "added_time",

  -- Present / Absent student lists: keep raw text + parse into array (comma or semicolon separated)
    NULLIF(trim(src."Present_Students_List"::text), '') AS present_students_list_text,
  CASE
    WHEN trim(src."Present_Students_List"::text) = '' THEN NULL
    ELSE regexp_split_to_array(trim(src."Present_Students_List"::text), '\s*[,;]\s*')
  END AS present_students_list_arr,

    NULLIF(trim(src."Absent_Students_List"::text), '') AS absent_students_list_text,
  CASE
    WHEN trim(src."Absent_Students_List"::text) = '' THEN NULL
    ELSE regexp_split_to_array(trim(src."Absent_Students_List"::text), '\s*[,;]\s*')
  END AS absent_students_list_arr,

  -- Volunteer engaged -> boolean + raw
    NULLIF(trim(src."Volunteer_Engaged"::text), '') AS volunteer_engaged_text,
  CASE
    WHEN lower(trim(src."Volunteer_Engaged"::text)) ~ '^(yes|y|true|1)$' THEN TRUE
    WHEN lower(trim(src."Volunteer_Engaged"::text)) ~ '^(no|n|false|0)$' THEN FALSE
    ELSE NULL
  END AS volunteer_engaged,

  -- Volunteering hours -> numeric (allow decimals)
  CASE
      WHEN trim(src."Volunteering_hrs_Engaged"::text) ~ '^[\\d\\.,]+$' THEN
        (regexp_replace(trim(src."Volunteering_hrs_Engaged"::text), ',', '.', 'g'))::NUMERIC
    ELSE NULL
  END AS volunteering_hours,

  -- Donor
    NULLIF(initcap(trim(src."Donor"::text)), '') AS donor,

  -- Keep the original raw row as jsonb to preserve any columns not explicitly selected
  to_jsonb(src) AS raw_record

FROM {{ source('STP_25-26', 'All_Session_Attendances') }} AS src
Where "ID" IS NOT NULL

