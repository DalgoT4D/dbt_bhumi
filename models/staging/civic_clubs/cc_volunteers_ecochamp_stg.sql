{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}


SELECT
    "Volunteer_Name" as volunteer_name,
    CAST("Volunteer_Hours" AS DECIMAL(10,2)) AS total_volunteer_hours,
    CAST("Total_Sessions_Attended" AS INTEGER) AS Total_Sessions_Attended,
    CAST(REPLACE("Attendance_Percentage", '%', '') AS DECIMAL(5,2)) AS attendance_percentage,
    "Volunteers_Mapped__School_Shelter_Name_" AS school_shelter_name,
    "Chapter" as city
FROM {{ source('ecochamps25_26', 'Center_Coordiantors') }}
where "Volunteer_Name" is not null and btrim("Volunteer_Name") != ''