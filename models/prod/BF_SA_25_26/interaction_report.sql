{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

with fellow_school as (
    select distinct
        academic_year,
        fellow_id,
        fellow_name,
        cohort,
        pm_name,
        school_name,
        donor_name
    from {{ ref('fellow_scl_cls_data') }}
    where school_name is not null
),

periods as (
    select
        academic_year,
        month,
        quarter
    from {{ ref('calender_year') }}
),

base as (
    select
        fs.academic_year,
        p.month,
        p.quarter,
        fs.fellow_id,
        fs.fellow_name,
        fs.cohort,
        fs.pm_name as pm,
        fs.school_name as school,
        fs.donor_name
    from fellow_school as fs
    inner join periods as p
        on fs.academic_year = p.academic_year
),

odc as (
    select
        academic_year,
        month,
        quarter,
        fellow_id,
        school_name,
        sum(odc_count) as odc_count
    from {{ ref('fellow_odc_int') }}
    group by
        academic_year,
        month,
        quarter,
        fellow_id,
        school_name
),

checkins as (
    select
        academic_year,
        month,
        quarter,
        fellow_id,
        school_name,
        sum(checkin_count) as checkin_count
    from {{ ref('fellow_checkins') }}
    group by
        academic_year,
        month,
        quarter,
        fellow_id,
        school_name
),

fcm_updates as (
    select
        academic_year,
        month,
        quarter,
        fellow_id,
        school as school_name,
        count(distinct fcm_update_id) as fcm_update_count
    from {{ ref('fcm_group_int') }}
    group by
        academic_year,
        month,
        quarter,
        fellow_id,
        school
),

classroom_updates as (
    select
        academic_year,
        month,
        quarter,
        fellow_id,
        school_name,
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
        school_name
)

select
    b.fellow_name,
    b.cohort,
    b.pm,
    b.school,
    b.donor_name,
    b.month,
    b.quarter,
    o.odc_count,
    c.checkin_count,
    f.fcm_update_count,
    cu.school_leader_checkin_count,
    cu.teacher_circle_count,
    cu.community_visit_count,
    cu.teaching_hours_sum,
    cu.helo_circles_count,
    cu.ptms_count
from base as b
left join odc as o
    on
        b.academic_year = o.academic_year
        and b.month = o.month
        and b.quarter = o.quarter
        and b.fellow_id = o.fellow_id
        and b.school = o.school_name
left join checkins as c
    on
        b.academic_year = c.academic_year
        and b.month = c.month
        and b.quarter = c.quarter
        and b.fellow_id = c.fellow_id
        and b.school = c.school_name
left join fcm_updates as f
    on
        b.academic_year = f.academic_year
        and b.month = f.month
        and b.quarter = f.quarter
        and b.fellow_id = f.fellow_id
        and b.school = f.school_name
left join classroom_updates as cu
    on
        b.academic_year = cu.academic_year
        and b.month = cu.month
        and b.quarter = cu.quarter
        and b.fellow_id = cu.fellow_id
        and b.school = cu.school_name
