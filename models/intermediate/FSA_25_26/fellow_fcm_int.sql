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

fcm_updates as (
    select
        academic_year,
        month,
        quarter,
        fellow_id,
        cohort,
        year_map,
        school,
        grade,
        grade_section,
        avg(fcm_avg) as fcm_avg,
        count(distinct fcm_update_id) as fcm_update_count
        
    from {{ ref('fcm_group_int') }}
    group by
        academic_year,
        month,
        quarter,
        fellow_id,
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
    fcm.fcm_avg,
    fcm.fcm_update_count
from fellow_school_class as fs
left join fcm_updates as fcm
    on
        fs.academic_year = fcm.academic_year
        and fs.funding_year = fcm.year_map
        and fs.fellow_id = fcm.fellow_id
        and fs.cohort = fcm.cohort
        and fs.school_name = fcm.school
        and fs.grade = fcm.grade
        and fs.grade_section = fcm.grade_section
        and fs.month = fcm.month
        and fs.quarter = fcm.quarter