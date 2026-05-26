{{ config(
  materialized='table',
  tags=["civic_clubs", "staging"]
) }}

SELECT
    "Full_Name" AS full_name,
    to_timestamp("Added_Time"::text, 'DD-Mon-YYYY HH24:MI:SS')::date AS added_date,
    "Location" AS location,
    "HR_Status" AS hr_status
FROM
    {{ source('zc_bvms_data', 'Website_Registration_Admin') }}
WHERE "Full_Name" IS NOT null AND btrim("Full_Name") != ''
