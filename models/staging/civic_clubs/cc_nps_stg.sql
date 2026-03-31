{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

select
"Name" as name,
"Email" as email,
cast("Rating" as numeric) as rating,
"Quarter" as quarter,
"program" as program,
"Feedback" as feedback,
lower(replace("utm_source", '"', '')) as utm_source,
"Sub_Programs" as sub_programs,
to_timestamp("Submission_Time", 'YYYY-MM-DD HH24:MI:SS')::date as submission_date

from {{ source('civic_clubs', 'Submission') }}