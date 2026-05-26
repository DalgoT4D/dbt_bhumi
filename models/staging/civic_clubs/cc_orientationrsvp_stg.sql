{{ config(
  materialized='table',
  tags=["civic_clubs", "staging"]
) }}

SELECT
    "Volunteer_Registration_First_Name" AS first_name,
    to_timestamp("Added_Time"::text, 'DD-Mon-YYYY HH24:MI:SS')::date AS added_date,
    "Volunteer_Registration_City" ->> 'City' AS registration_city,
    "Event_Name_City" ->> 'City' AS event_city,
    "Event_Name_Event_Start_Date" AS event_start_date,
    "Volunteer_Registration_Registration_Status" AS registration_status,
    "Volunteer_Registration_Catalyse_last_activity_date" AS last_activity_date
FROM
    {{ source('zc_bvms_data', 'Orientation_RSVP_Admin') }}
WHERE "Volunteer_Registration_First_Name" IS NOT null AND btrim("Volunteer_Registration_First_Name") != ''
