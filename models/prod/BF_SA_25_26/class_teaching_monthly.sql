{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

with class_upd_int as (
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
        teaching_hours
    from {{ ref('class_upd_int') }}
),

teaching_hours as (
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
        city,
        school_type,
        grade,
        grade_section,
        'Teaching Hours' as parameters,

        case
            when teaching_hours is null then 'Black'
            when teaching_hours <= 9 then 'Red'
            when teaching_hours >= 10 and teaching_hours <= 12 then 'Amber'
            when teaching_hours >= 13 then 'Green'
        end as brag
    from class_upd_int
)

select * from teaching_hours
