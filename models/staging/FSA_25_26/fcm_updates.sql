{{ config(
  materialized='table',
  tags=["fsa_25_26", "staging"]
) }}

with fcm_updates as (
    select 
        NULLIF(BTRIM(id::TEXT),'') as fcm_update_id,
        NULLIF(BTRIM(pm_id::TEXT),'') as pm_id,
        NULLIF(BTRIM(cohort::TEXT),'') as cohort,
        NULLIF(BTRIM(school::TEXT),'') as school,
        NULLIF(BTRIM(fellow_id::TEXT),'') as fellow_id,
        REGEXP_REPLACE(BTRIM(grade_section::TEXT), '[^0-9]', '', 'g') as grade,
        LOWER(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(BTRIM(grade_section::TEXT), ''), '-', '', 'g'), '([0-9])([a-zA-Z])', '\1 \2', 'g')) as grade_section,
        -- NULLIF(BTRIM(reporting_date::TEXT),'')::DATE as reporting_date,
        -- TO_CHAR(reporting_date, 'Mon YYYY') as month_year,
        NULLIF(BTRIM(reporting_period::TEXT),'') as reporting_period

    from {{ source('fellowship_school_app_25_26', 'fcm_updates_25_26') }}
)

select 
    fcm_update_id,
    pm_id,
    cohort,
    school,
    fellow_id,
    grade,
    grade_section,
    reporting_period
from fcm_updates
where
    fcm_update_id is not NULL
    and fellow_id is not NULL
