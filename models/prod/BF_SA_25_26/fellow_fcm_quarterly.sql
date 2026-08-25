{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

with fellow_data as (
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
        grade_section
    from {{ ref('fellow_scl_cls_data') }}
),

-- select * from fellow_data

calender_year as (
    select distinct quarter
    from {{ ref('calender_year') }}
),

fellow_data_quarter as (
    select
        fd.academic_year,
        cy.quarter,
        fd.fellow_id,
        fd.fellow_name,
        fd.pm_name,
        fd.cohort,
        fd.funding_year, 
        fd.donor_name,
        fd.school_name,
        fd.school_state,
        fd.city,
        fd.school_type,
        fd.grade, 
        fd.grade_section
    from fellow_data as fd
    cross join calender_year as cy
),

-- select * from school_quarter

fellow_fcm_avg as (
    select
        academic_year,
        quarter,
        fellow_id,
        cohort,
        year_map,
        school,
        grade,
        grade_section,
        avg(fcm_avg) as fcm_avg
    from {{ ref('fcm_group_int') }}
    group by
        academic_year,
        quarter,
        fellow_id,
        cohort,
        year_map,
        school,
        grade,
        grade_section
),

join_school_year as (
    select 
        fdq.academic_year,
        fdq.quarter,
        fdq.fellow_id,
        fdq.fellow_name,
        fdq.pm_name,
        fdq.cohort,
        fdq.funding_year,
        fdq.donor_name,
        fdq.school_name,
        fdq.school_state,
        fdq.school_type,
        fdq.city,
        fdq.grade,
        fdq.grade_section,
        fo.fcm_avg
    from fellow_data_quarter as fdq
    left join fellow_fcm_avg as fo
        on
            fdq.academic_year = fo.academic_year
            and fdq.quarter = fo.quarter
            and fdq.funding_year = fo.year_map
            and fdq.fellow_id = fo.fellow_id
            and fdq.school_name = fo.school
            and fdq.grade = fo.grade
            and fdq.grade_section = fo.grade_section

),

-- select * from join_school_year

fcm_avg as (
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
        'FCM' as parameters,

        case
            when fcm_avg is null then 'Black'
            when fcm_avg <= 49 then 'Red'
            when fcm_avg >= 50 and fcm_avg <= 74 then 'Amber'
            when fcm_avg >= 75 then 'Green'
        end as brag
    from join_school_year
)

select * from fcm_avg
