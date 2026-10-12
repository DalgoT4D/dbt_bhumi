{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

with fellow_checkins as (
    select 
        academic_year,
        quarter,
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
        sum(checkin_count) as checkins_count
    from {{ ref('fellow_checkins') }}
    group by
        academic_year,
        quarter,
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
        grade_section
),


checkins_count as (
    select 
        academic_year,
        quarter,
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
            when checkins_count <= 2 then 'Red'
            when checkins_count >= 3 and checkins_count <= 5 then 'Amber'
            when checkins_count >= 6 then 'Green'
        end as brag
    from fellow_checkins
)

select *
from checkins_count

union all

select *
from {{ ref('fellow_fcm_quarterly') }}

union all

select *
from {{ ref('fellow_odc_quarterly') }}
