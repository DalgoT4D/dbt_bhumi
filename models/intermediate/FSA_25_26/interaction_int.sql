{{ config(
  materialized='table',
  tags=["fsa_25_26", "int"]
) }}

WITH fellow_school AS (
    SELECT
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
    FROM {{ ref('fy_fellow_sch_cls_int') }}
),

odc as (
    select
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        pm_name,
        funding_year,
        cohort,
        school_name,
        grade,
        grade_section,
        sum(odc_count) as odc_count
    from {{ ref('fellow_odc_int') }}
    group by
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        pm_name,
        funding_year,
        cohort,
        school_name,
        grade,
        grade_section
),

checkins as (
    select
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        pm_name,
        funding_year,
        cohort,
        school_name,
        grade,
        grade_section,
        sum(checkin_count) as checkin_count
    from {{ ref('fellow_checkins') }}
    group by
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        pm_name,
        funding_year,
        cohort,
        school_name,
        grade,
        grade_section
),

fcm_updates as (
    select
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        pm_name,
        funding_year,
        cohort,
        school_name,
        grade,
        grade_section,
        sum(fcm_update_count) as fcm_update_count
    from {{ ref('fellow_fcm_int') }}
    group by
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        pm_name,
        funding_year,
        cohort,
        school_name,
        grade,
        grade_section
),

classroom_updates as (
    select
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        pm_name,
        funding_year,
        cohort,
        school_name,
        grade,
        grade_section,
        sum(school_leader_checkins) as school_leader_checkin_count,
        sum(teacher_circles) as teacher_circle_count,
        sum(homes_visited) as community_visit_count,
        sum(teaching_hours) as teaching_hours_sum,
        sum(helo_circles) as helo_circles_count,
        sum(ptms) as ptms_count
    from {{ ref('class_upd_int') }}
    group by
        academic_year,
        month,
        quarter,
        fellow_id,
        fellow_name,
        pm_name,
        funding_year,
        cohort,
        school_name,
        grade,
        grade_section
)

select
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
    o.odc_count,
    c.checkin_count,
    f.fcm_update_count,
    cu.school_leader_checkin_count,
    cu.teacher_circle_count,
    cu.community_visit_count,
    cu.teaching_hours_sum,
    cu.helo_circles_count,
    cu.ptms_count
from fellow_school as fs
left join odc as o
    on
        fs.academic_year = o.academic_year
        and fs.month = o.month
        and fs.quarter = o.quarter
        and fs.fellow_id = o.fellow_id
        and fs.school_name = o.school_name
        and fs.grade = o.grade
        and fs.grade_section = o.grade_section
left join checkins as c
    on
        fs.academic_year = c.academic_year
        and fs.month = c.month
        and fs.quarter = c.quarter
        and fs.fellow_id = c.fellow_id
        and fs.school_name = c.school_name
        and fs.grade = c.grade
        and fs.grade_section = c.grade_section
left join fcm_updates as f
    on
        fs.academic_year = f.academic_year
        and fs.month = f.month
        and fs.quarter = f.quarter
        and fs.fellow_id = f.fellow_id
        and fs.school_name = f.school_name
        and fs.grade = f.grade
        and fs.grade_section = f.grade_section
left join classroom_updates as cu
    on
        fs.academic_year = cu.academic_year
        and fs.month = cu.month
        and fs.quarter = cu.quarter
        and fs.fellow_id = cu.fellow_id
        and fs.school_name = cu.school_name
        and fs.grade = cu.grade
        and fs.grade_section = cu.grade_section
