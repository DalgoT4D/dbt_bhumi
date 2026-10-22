{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

with fellow_checkins as (
    select 
        academic_year,
        month,
        fellow_id,
        fellow_name,
        pm_name,
        cohort,
        funding_year,
        donor_name,
        school_name,
        school_state,
        school_type,
        city,
        grade,
        grade_section,
        checkins_count
    from {{ ref('fellow_checkins') }}
),


checkins_count as (
    select 
        academic_year,
        month,
        fellow_id,
        fellow_name,
        pm_name,
        cohort,
        funding_year,
        donor_name,
        school_name,
        school_state,
        school_type,
        city,
        grade,
        grade_section,
        'Checkinks' as parameters,

        case
            when checkins_count is null then 'Black'
            when checkins_count = 0 then 'Red'
            when checkins_count = 1 then 'Amber'
            when checkins_count >= 2 then 'Green'
        end as brag
    from fellow_checkins
)

select *
from checkins_count

union all

select *
from {{ ref('fellow_odc_monthly') }}
