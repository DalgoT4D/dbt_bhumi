{{ config(
  materialized='table',
  tags=["fsa_25_26", "staging"]
) }}

with school_fy_data as (
    select
        nullif(btrim(school_id::text), '') as school_id,
        nullif(btrim(funding_year::text),'') as funding_year,
        nullif(btrim(male_students::text), '')::integer as male_students,
        nullif(btrim(total_students::text), '')::integer as total_students,
        nullif(btrim(female_students::text), '')::integer as female_students,
        nullif(btrim(total_support_staff::text), '')::integer as total_support_staff,
        nullif(btrim(total_teaching_staff::text), '')::integer as total_teaching_staff
    from {{ source('fellowship_school_app_25_26', 'school_fy_data') }}
)

select *
from school_fy_data
where school_id is not null
