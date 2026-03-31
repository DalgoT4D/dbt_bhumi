{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

select 

"Attendance" as attendance,
"Event_Name" ->> 'Event_Name' as event_name,
"Event_Name" ->> 'Event_Type' as event_type,
"Event_Name" ->> 'Event_Start_Date' as event_start_date,
"Event_Name_City" ->> 'City' as city,
"Event_Name_State" ->> 'State' as state,
cast("Event_Name_Event_Duration" as numeric) as event_duration,
"Volunteer_Registration_ID" as volunteer_id

from 

{{ source('zc_bvms_data', 'Volunteer_Event_Registration_Catalyse1') }}


