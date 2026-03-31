{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}


SELECT
    "Volunteer_Name" as volunteer_name,
    CAST("Volunteer_Hours" AS DECIMAL(10,2)) AS total_volunteer_hours,
    CAST(REPLACE("Attendance_Percentage", '%', '') AS DECIMAL(5,2)) AS attendance_percentage,
    "Volunteers_Mapped__School_Shelter_Name_" AS school_shelter_name
FROM {{ source('ecochamps25_26', 'Center_Coordiantors') }}