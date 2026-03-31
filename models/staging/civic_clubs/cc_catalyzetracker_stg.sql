{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

SELECT 
"Event" ->> 'Event_Name' as event_name,
"Event" ->> 'Event_Start_Date' as event_date,
"Impact_1" ->> 'Outcome' as outcome_1,
"Impact_2" ->> 'Outcome' as outcome_2,
"Impact_3" ->> 'Outcome' as outcome_3,
cast(nullif("Quantity_1", '') as numeric) as quantity_1,
cast(nullif("Quantity_2", '') as numeric) as quantity_2,
cast(nullif("Quantity_3", '') as numeric) as quantity_3,

"Event_City" ->> 'City' as city,
"Event_Cause1" ->> 'Cause' as cause,
"Event_Event_For" ->> 'Event_List' as event_list,
"Event_Event_Status" as event_status,
"Event_Event_Category" ->>'Event_Category' as event_category,
cast(nullif("Total_Volunteer_Hours", '') as numeric) as total_volunteer_hours,
cast(nullif("Acual_number_of_volunteers", '') as numeric) as number_of_volunteers

from 
{{ source('zc_bvms_data', 'Catalyse_Tracker_Catalyse') }}
