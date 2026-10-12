{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

with fellow_odc as (
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
        sum(odc_count) as odc_count
    from {{ ref('fellow_odc_int') }}
    group by
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
        grade_section
),

odc_count as (
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
        'ODCs' as parameters,

        case
            when odc_count is null then 'Black'
            when odc_count = 0 then 'Red'
            when odc_count = 1 then 'Amber'
            when odc_count >= 2 then 'Green'
        end as brag
    from fellow_odc
)

select * from odc_count
