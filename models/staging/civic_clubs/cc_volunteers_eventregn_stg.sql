{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

select 

"Volunteer_Registration_ID" as volunteer_id,
"Name" as name,
"Attendance" as attendance,
"Event_Name" ->> 'Event_Name' as event_name,
"Event_Name" ->> 'Event_Type' as event_type,
"Event_Name" ->> 'Event_Start_Date' as event_date,
"Event_Name_City" ->> 'City' as city,
"Event_Name_State" ->> 'State' as state,
CASE WHEN BTRIM("Event_Name_Event_Duration"::TEXT) ~ '^[0-9.]+$' THEN "Event_Name_Event_Duration"::NUMERIC END AS event_duration


from 

{{ source('zc_bvms_data', 'Volunteer_Event_Registration_Catalyse1') }}


