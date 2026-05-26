{{ config(
  materialized='table',
  tags=["civic_clubs", "staging"]
) }}

select
    "Name" as name,
    "Email" as email,
    cast("Rating" as numeric) as rating,
    "Quarter" as quarter,
    program,
    "Feedback" as feedback,
    lower(replace(utm_source, '"', '')) as utm_source,
    "Sub_Programs" as sub_programs,
    cast(to_timestamp("Submission_Time", 'YYYY-MM-DD HH24:MI:SS') as date) as submission_date

from {{ source('civic_clubs', 'Submission') }}
where
    "Name" is not null
    and "Email" is not null
    and lower("Feedback") not like '%test%'
