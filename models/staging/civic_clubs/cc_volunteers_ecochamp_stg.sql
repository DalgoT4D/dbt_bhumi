{{ config(
  materialized='table',
  tags=["civic_clubs", "staging"]
) }}


SELECT
    "Volunteer_Name" AS volunteer_name,
    CAST("Volunteer_Hours" AS DECIMAL(10,2)) AS total_volunteer_hours,
    CAST("Total_Sessions_Attended" AS INTEGER) AS total_sessions_attended,
    CAST(REPLACE("Attendance_Percentage", '%', '') AS DECIMAL(5,2)) AS attendance_percentage,
    "Volunteers_Mapped__School_Shelter_Name_" AS school_shelter_name,
    "Chapter" AS city
FROM {{ source('ecochamps25_26', 'Center_Coordiantors') }}
WHERE "Volunteer_Name" IS NOT null AND BTRIM("Volunteer_Name") != ''
