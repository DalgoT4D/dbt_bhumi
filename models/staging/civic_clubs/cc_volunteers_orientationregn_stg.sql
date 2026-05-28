{{ config(
  materialized='table',
  tags=["civic_clubs", "staging"]
) }}

SELECT
    "Name" AS name,
    to_timestamp("Added_Time"::text, 'DD-Mon-YYYY HH24:MI:SS')::date AS added_date,
    "Attendance" AS attendance,
    "Event_Name" ->> 'Event_Name' AS event_name,
    "Event_Name" ->> 'Event_Type' AS event_type,
    "Event_Name" ->> 'Event_Start_Date' AS event_start_date,
    utm_source,
    "Event_Name_ID" AS event_name_id,
    "Event_Name_City" ->> 'City' AS city,
    "Event_Name_State" ->> 'State' AS state
FROM
    {{ source('zc_bvms_data', 'Orienation_Volunteer_Event_Registration') }}
WHERE "Name" IS NOT NULL AND btrim("Name") != ''
