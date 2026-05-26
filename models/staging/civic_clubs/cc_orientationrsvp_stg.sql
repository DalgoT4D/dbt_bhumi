{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

SELECT
"Volunteer_Registration_First_Name" as first_name,
to_timestamp("Added_Time"::text, 'DD-Mon-YYYY HH24:MI:SS')::date as added_date,
"Volunteer_Registration_City" ->> 'City' as registration_city,
"Event_Name_City" ->> 'City' as event_city,
"Event_Name_Event_Start_Date" as event_start_date,
"Volunteer_Registration_Registration_Status" as registration_status,
"Volunteer_Registration_Catalyse_last_activity_date" as last_activity_date
FROM
{{ source('zc_bvms_data', 'Orientation_RSVP_Admin') }}
where "Volunteer_Registration_First_Name" is not null and btrim("Volunteer_Registration_First_Name") != ''