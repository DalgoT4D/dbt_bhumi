{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

SELECT
"Full_Name" as full_name,
to_timestamp("Added_Time"::text, 'DD-Mon-YYYY HH24:MI:SS')::date as added_date,
"Location" as location,
"HR_Status" as hr_status
FROM
{{ source('zc_bvms_data', 'Website_Registration_Admin') }}
where "Full_Name" is not null and btrim("Full_Name") != ''