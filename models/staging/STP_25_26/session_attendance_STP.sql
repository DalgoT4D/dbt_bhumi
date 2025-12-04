-- Cleaned STP 25-26 Session Attendances
-- Source: STP_25-26.All_Session_Attendances

SELECT
  -- canonical identifier
  CASE
    WHEN trim(src."id"::text) ~ '^\d+$' THEN trim(src."id"::text)::BIGINT
    ELSE NULL
  END AS id,

  -- Was this class combined with another section? -> normalized boolean + raw text
  CASE
    WHEN lower(trim(src."was_this_class_combined_with_another_section_"::text)) ~ '^(yes|y|true|1)$' THEN TRUE
    WHEN lower(trim(src."was_this_class_combined_with_another_section_"::text)) ~ '^(no|n|false|0)$' THEN FALSE
    ELSE NULL
  END AS was_class_combined,

  -- Academic Year (expecting format YYYY-YYYY)
  CASE
      WHEN trim(src."academic_year"::text) ~ '^\d{4}-\d{4}$' THEN trim(src."academic_year"::text)
    ELSE NULL
  END AS academic_year,

  -- School & class info
    NULLIF(initcap(trim(src."school_name"::text)), '') AS school_name,
    NULLIF(initcap(trim(src."class_type"::text)), '') AS class_type,
    NULLIF(initcap(trim(src."trainer"::text)), '') AS trainer_name,
    NULLIF(trim(src."class"::text), '') AS class,
    NULLIF(trim(src."class_division"::text), '') AS class_division,

  -- Intervention and session metadata
    NULLIF(initcap(trim(src."intervension_name"::text)), '') AS intervention_name,
    NULLIF(initcap(trim(src."session_type"::text)), '') AS session_type,

  -- session_date: try several common formats, return as date
  CASE
    WHEN trim(src."session_date"::text) ~ '^\d{4}-\d{2}-\d{2}$' THEN to_date(trim(src."session_date"::text), 'YYYY-MM-DD')
    WHEN trim(src."session_date"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(trim(src."session_date"::text), 'DD/MM/YYYY')
    WHEN trim(src."session_date"::text) ~ '^\d{1,2}-[A-Za-z]{3}-\d{2,4}$' THEN to_date(trim(src."session_date"::text), 'DD-Mon-YYYY')
    WHEN trim(src."session_date"::text) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}(:\d{2})?$' THEN to_timestamp(trim(src."session_date"::text), 'YYYY-MM-DD HH24:MI:SS')::date
    ELSE NULL
  END AS session_date,

  -- Present / Absent / Total -> integers
    CASE WHEN trim(src."present_students"::text) ~ '^\d+$' THEN trim(src."present_students"::text)::INT ELSE NULL END AS present_students,
    CASE WHEN trim(src."absent_students"::text) ~ '^\d+$' THEN trim(src."absent_students"::text)::INT ELSE NULL END AS absent_students,
    CASE WHEN trim(src."total_student"::text) ~ '^\d+$' THEN trim(src."total_student"::text)::INT ELSE NULL END AS total_students,

  --"added_time -> timestamp
  CASE
        WHEN trim(src."added_time"::text) ~ '^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}$' THEN (trim(src."added_time"::text))::timestamp
        WHEN trim(src."added_time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}\s+\d{2}:\d{2}:\d{2}$' THEN to_timestamp(trim(src."added_time"::text), 'DD/MM/YYYY HH24:MI:SS')
        WHEN trim(src."added_time"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN to_date(trim(src."added_time"::text), 'DD/MM/YYYY')::timestamp
    ELSE NULL
  END AS"added_time",

  -- Present / Absent student lists: keep raw text + parse into array (comma or semicolon separated)
    NULLIF(trim(src."present_students_list"::text), '') AS present_students_list_text,
  CASE
    WHEN trim(src."present_students_list"::text) = '' THEN NULL
    ELSE regexp_split_to_array(trim(src."present_students_list"::text), '\s*[,;]\s*')
  END AS present_students_list_arr,

    NULLIF(trim(src."absent_students_list"::text), '') AS absent_students_list_text,
  CASE
    WHEN trim(src."absent_students_list"::text) = '' THEN NULL
    ELSE regexp_split_to_array(trim(src."absent_students_list"::text), '\s*[,;]\s*')
  END AS absent_students_list_arr,

  -- Volunteer engaged -> boolean + raw
    NULLIF(trim(src."volunteer_engaged"::text), '') AS volunteer_engaged_text,
  CASE
    WHEN lower(trim(src."volunteer_engaged"::text)) ~ '^(yes|y|true|1)$' THEN TRUE
    WHEN lower(trim(src."volunteer_engaged"::text)) ~ '^(no|n|false|0)$' THEN FALSE
    ELSE NULL
  END AS volunteer_engaged,

  -- Volunteering hours -> numeric (allow decimals)
  CASE
      WHEN trim(src."volunteering_hrs_engaged"::text) ~ '^[\\d\\.,]+$' THEN
        (regexp_replace(trim(src."volunteering_hrs_engaged"::text), ',', '.', 'g'))::NUMERIC
    ELSE NULL
  END AS volunteering_hours,

  -- Donor
    NULLIF(initcap(trim(src."donor"::text)), '') AS donor,

  -- Keep the original raw row as jsonb to preserve any columns not explicitly selected
  to_jsonb(src) AS raw_record

FROM {{ source('STP_25-26', 'All_Session_Attendances') }} AS src
Where id IS NOT NUll

