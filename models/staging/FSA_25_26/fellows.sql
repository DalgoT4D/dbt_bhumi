{{ config(
  materialized='table',
  tags=["fsa_25_26", "staging"]
) }}

with fellows_base as (
    select
        NULLIF(BTRIM(id::TEXT),'') as fellow_id,
        NULLIF(BTRIM(pm_id::TEXT), '') as pm_id,
        NULLIF(BTRIM(cohort_year::TEXT),'') as cohort_year,
        NULLIF(BTRIM(employee_id::TEXT), '') as employee_id
        -- case
        --     when NULLIF(BTRIM(date_of_joining::TEXT),'') is null then null
        --     when BTRIM(date_of_joining::TEXT) ~ '^\\d{4}-\\d{2}-\\d{2}$' then BTRIM(date_of_joining::TEXT)::DATE
        --     when BTRIM(date_of_joining::TEXT) ~ '^\\d{2}/\\d{2}/\\d{4}$' then TO_DATE(BTRIM(date_of_joining::TEXT),'DD/MM/YYYY')
        --     when BTRIM(date_of_joining::TEXT) ~ '^\\d{2}-\\d{2}-\\d{4}$' then TO_DATE(BTRIM(date_of_joining::TEXT),'DD-MM-YYYY')
        -- end as date_of_joining,
        -- case
        --     when NULLIF(BTRIM(date_of_leaving::TEXT),'') is null then null
        --     when BTRIM(date_of_leaving::TEXT) ~ '^\\d{4}-\\d{2}-\\d{2}$' then BTRIM(date_of_leaving::TEXT)::DATE
        --     when BTRIM(date_of_leaving::TEXT) ~ '^\\d{2}/\\d{2}/\\d{4}$' then TO_DATE(BTRIM(date_of_leaving::TEXT),'DD/MM/YYYY')
        --     when BTRIM(date_of_leaving::TEXT) ~ '^\\d{2}-\\d{2}-\\d{4}$' then TO_DATE(BTRIM(date_of_leaving::TEXT),'DD-MM-YYYY')
        -- end as date_of_leaving,
        -- case
        --     when NULLIF(BTRIM(date_of_birth::TEXT),'') is null then null
        --     when BTRIM(date_of_birth::TEXT) ~ '^\\d{4}-\\d{2}-\\d{2}$' then BTRIM(date_of_birth::TEXT)::DATE
        --     when BTRIM(date_of_birth::TEXT) ~ '^\\d{2}/\\d{2}/\\d{4}$' then TO_DATE(BTRIM(date_of_birth::TEXT),'DD/MM/YYYY')
        --     when BTRIM(date_of_birth::TEXT) ~ '^\\d{2}-\\d{2}-\\d{4}$' then TO_DATE(BTRIM(date_of_birth::TEXT),'DD-MM-YYYY')
        -- end as date_of_birth
    from {{ source('fellowship_school_app_25_26', 'fellows_raw_data') }}
),

-- select distinct * from fellows_base
-- where
--     fellow_id is not null
--     and employee_id != 'FAKE001'

max_cohort as (
    select MAX(cohort_year::INTEGER) as max_cohort_year
    from fellows_base
    where cohort_year ~ '^\d{4}$'
),

fellows as (
    select
        fb.*,
        funding_years.funding_year
    from fellows_base as fb
    cross join max_cohort as mc
    cross join
        lateral (
            values
            (fb.cohort_year),
            (
                case
                    when
                        fb.cohort_year ~ '^\d{4}$'
                        and fb.cohort_year::INTEGER < mc.max_cohort_year
                        then (fb.cohort_year::INTEGER + 1)::TEXT
                end
            )
        ) as funding_years(funding_year)
    where funding_years.funding_year is not null
)

select distinct * from fellows
where
    fellow_id is not null
    and employee_id != 'FAKE001' 
