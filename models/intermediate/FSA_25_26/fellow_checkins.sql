{{ config(
  materialized='table',
  tags=["fsa_25_26", "int"]
) }}

with fellow_school_class as (
    select
        academic_year,
        fellow_id,
        fellow_name,
        pm_id,
        pm_name,
        cohort,
        funding_year,
        donor_id,
        donor_name,
        school_id,
        school_name,
        school_state,
        city,
        school_type,
        grade,
        grade_section
    from {{ ref('fellow_scl_cls_data') }}
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
        count(*) as checkin_count
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
    fsc.academic_year,
    c.month,
    c.quarter,
    fsc.fellow_id,
    fsc.fellow_name,
    fsc.pm_id,
    fsc.pm_name,
    fsc.cohort,
    fsc.funding_year,
    fsc.school_name,
    fsc.grade,
    fsc.grade_section,
    fsc.school_state,
    fsc.city,
    fsc.school_type,
    fsc.donor_name,
    c.checkin_count
from fellow_school_class as fsc
left join checkins as c
    on
        fsc.academic_year = c.academic_year
        and fsc.funding_year = c.year_map
        and fsc.fellow_id = c.fellow_id
        and fsc.school_name = c.school
        and fsc.grade = c.grade
        and fsc.grade_section = c.grade_section
