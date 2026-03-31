{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}


select
"Name" as leader_name,
"Role" as role,
"Current_Location" as city,
"Chapter_District" as chapter_district,
"Clubs" as clubs,
"Continuing" as continuing

from {{ source('civic_clubs', 'Active_Leaders') }}