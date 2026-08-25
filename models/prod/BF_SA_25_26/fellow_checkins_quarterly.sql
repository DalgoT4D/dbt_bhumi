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

fellow_checkins as (
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
        sum(checkin_count) as checkins_count
    from {{ ref('fellow_checkins') }}
    group by
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
        fc.checkins_count
    from fellow_data_quarter as fdq
    left join fellow_checkins as fc
        on
            fdq.academic_year = fc.academic_year
            and fdq.quarter = fc.quarter
            and fdq.funding_year = fc.funding_year
            and fdq.fellow_id = fc.fellow_id
            and fdq.school_name = fc.school_name
            and fdq.grade = fc.grade
            and fdq.grade_section = fc.grade_section

),

-- select * from join_school_year

checkins_count as (
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
        'Checkinks' as parameters,

        case
            when checkins_count is null then 'Black'
            when checkins_count <= 2 then 'Red'
            when checkins_count >= 3 and checkins_count <= 5 then 'Amber'
            when checkins_count >= 6 then 'Green'
        end as brag
    from join_school_year
)

select * from checkins_count
