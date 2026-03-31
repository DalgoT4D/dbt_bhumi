{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

SELECT
"Full_Name" as full_name,
"Added_Time" as added_date,
"Location" as location,
"HR_Status" as hr_status
FROM
{{ source('zc_bvms_data', 'Website_Registration_Admin') }}