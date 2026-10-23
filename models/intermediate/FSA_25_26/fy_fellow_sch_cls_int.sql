{{ config(
  materialized='table',
  tags=["fsa_25_26", "int"]
) }}

with fellow_school_class_data as (
    select distinct
        academic_year,
        fellow_id,
        fellow_name,
        cohort,
        funding_year, 
        donor_name,
        pm_name,
        school_name,
        school_state,
        city,
        school_type,
        grade, 
        no_of_students,
        grade_section
    from {{ ref('fellow_scl_cls_data') }}
),

-- select * from school_data

calender_year as (
    select distinct
        quarter,
        month
    from {{ ref('calender_year') }}
),

fy_fellow_school_class_data as (
    select
        fa.academic_year,
        cy.quarter,
        cy.month,
        fa.fellow_id,
        fa.fellow_name,
        fa.cohort,
        fa.funding_year, 
        fa.donor_name,
        fa.pm_name,
        fa.school_name,
        fa.school_state,
        fa.city,
        fa.school_type,
        fa.grade, 
        fa.no_of_students,
        fa.grade_section
    from fellow_school_class_data as fa
    cross join calender_year as cy
)

select *
from fy_fellow_school_class_data
