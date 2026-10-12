{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}


with fellow_odc_int as (
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
        avg(student_engagement_percentage) as student_engagement
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

student_engagemnt as (
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
        'Student Engagement' as parameters,

        case
            when student_engagement is null then 'Black'
            when student_engagement <= 49 then 'Red'
            when student_engagement >= 50 and student_engagement <= 74 then 'Amber'
            when student_engagement >= 75 then 'Green'
        end as brag
    from fellow_odc_int
)

select *
from student_engagemnt

union all

select *
from {{ ref('class_teaching_monthly') }}
