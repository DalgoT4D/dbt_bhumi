{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}


select 
"ID" as id,
case
  when "City" = 'BLR' then 'Bengaluru'
  else "City"
end as city,
"Name" ->> 'first_name' as volunteer_name,
"Award" as award,
"Year1" as year,
"Project" ->> 'Project_Code' as project_code,
"Project_Name" as project_name

from {{ source('zc_bvms_data', 'Shelters_Historic_Volunteer_Report') }}