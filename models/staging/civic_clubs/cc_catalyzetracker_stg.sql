{{ config(
  materialized='table',
  tags=["civic_clubs", "staging"]
) }}

SELECT 
    "Event" ->> 'ID' AS event_id,
    "Event" ->> 'Event_Name' AS event_name,
    "Event" ->> 'Event_Start_Date' AS event_date,
    "Impact_1" ->> 'Outcome' AS outcome_1,
    "Impact_2" ->> 'Outcome' AS outcome_2,
    "Impact_3" ->> 'Outcome' AS outcome_3,
    cast(nullif("Quantity_1", '') AS numeric) AS quantity_1,
    cast(nullif("Quantity_2", '') AS numeric) AS quantity_2,
    cast(nullif("Quantity_3", '') AS numeric) AS quantity_3,

    "Event_City" ->> 'City' AS city,
    "Event_Cause1" ->> 'Cause' AS cause,
    "Event_Event_For" ->> 'Event_List' AS event_list,
    "Event_Event_Status" AS event_status,
    "Event_Event_Category" ->>'Event_Category' AS event_category,
    cast(nullif("Total_Volunteer_Hours", '') AS numeric) AS total_volunteer_hours,
    cast(nullif("Acual_number_of_volunteers", '') AS numeric) AS number_of_volunteers,
    "Event_Club_City_and_Names" ->> 'ID' AS club_id,
    "Event_Club_City_and_Names" ->> 'Club_Name' AS club_name,
    "Event_Club_City_and_Names" ->> 'zc_display_value' AS club_display_value

FROM 
    {{ source('zc_bvms_data', 'Catalyse_Tracker_Catalyse') }}
