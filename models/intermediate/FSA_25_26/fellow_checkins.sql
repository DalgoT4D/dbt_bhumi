{{ config(
  materialized='table',
  tags=["fsa_25_26", "int"]
) }}

with fellow_school_class as (
    select
        academic_year,
        quarter,
        month,
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
        grade_section
    from {{ ref('fy_fellow_sch_cls_int') }}
),

checkins as (
    select
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        cohort,
        year_map,
        school,
        grade,
        grade_section,
        COUNT(*) as checkin_count
    from {{ ref('checkins_fsa') }}
    group by
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        cohort,
        year_map,
        school,
        grade,
        grade_section
)

select distinct
    fs.academic_year,
    fs.month,
    fs.quarter,
    fs.fellow_id,
    fs.fellow_name,
    fs.pm_name,
    fs.cohort,
    fs.funding_year,
    fs.donor_name,
    fs.school_name,
    fs.school_state,
    fs.city,
    fs.school_type,
    fs.grade,
    fs.grade_section,
    c.checkin_count
from fellow_school_class as fs
left join checkins as c
    on
        fs.academic_year = c.academic_year
        and fs.funding_year = c.year_map
        and fs.fellow_id = c.fellow_id
        and fs.school_name = c.school
        and fs.grade = c.grade
        and fs.grade_section = c.grade_section
        and fs.month = c.month
        and fs.quarter = c.quarter
