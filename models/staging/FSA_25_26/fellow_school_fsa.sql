{{ config(
  materialized='table',
  tags=["fsa_25_26", "staging"]
) }}

with fellow_school as (
    select
        -- NULLIF(BTRIM(id::TEXT),'') as id,
        NULLIF(BTRIM(fellow_id::TEXT),'') as fellow_id,
        NULLIF(BTRIM(grade::TEXT),'') as grade,
        NULLIF(BTRIM(school_id::TEXT),'') as school_id,
        NULLIF(BTRIM(no_of_students::TEXT),'')::INTEGER as no_of_students,
        LOWER(NULLIF(BTRIM(section::TEXT),'')) as section,
        case
            when LOWER(NULLIF(BTRIM(section::TEXT),'')) is not NULL then NULLIF(BTRIM(grade::TEXT),'') || ' ' || LOWER(NULLIF(BTRIM(section::TEXT),''))
            else NULLIF(BTRIM(grade::TEXT),'')
        end as grade_section,
        NULLIF(BTRIM(funding_year::TEXT),'') as funding_year
    from {{ source('fellowship_school_app_25_26', 'fellow_school_grade') }}
)

select distinct 
    fellow_id,
    grade,
    school_id,
    no_of_students,
    section,
    grade_section,
    funding_year
from fellow_school
where
    fellow_id is not NULL
