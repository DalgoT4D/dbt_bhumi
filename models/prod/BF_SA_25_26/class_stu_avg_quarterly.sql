{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

with classroom_data as (
    select distinct
        academic_year,
        school_name,
        school_state,
        city,
        school_type,
        grade,
        grade_section
    from {{ ref('fellow_scl_cls_data') }}
    where
        school_name is not null
),

-- select * from school_data

calender_year as (
    select distinct quarter
    from {{ ref('calender_year') }}
),

classroom_quarter as (
    select
        cd.academic_year,
        cy.quarter,
        cd.school_name,
        cd.school_state,
        cd.city,
        cd.school_type,
        cd.grade,
        cd.grade_section
    from classroom_data as cd
    cross join calender_year as cy
),

-- select * from school_quarter

fellow_odc_int as (
    select
        academic_year,
        school_name,
        school_state,
        city,
        school_type,
        grade,
        grade_section,
        quarter,
        avg(student_engagement_percentage) as student_engagement
    from {{ ref('fellow_odc_int') }}
    group by
        academic_year,
        school_name,
        school_state,
        city,
        school_type,
        grade,
        grade_section,
        quarter
),

join_school_year as (
    select 
        sq.academic_year,
        sq.quarter,
        sq.school_name,
        sq.school_state,
        sq.city,
        sq.school_type,
        sq.grade,
        sq.grade_section,
        gsq.student_engagement
    from classroom_quarter as sq
    left join fellow_odc_int as gsq
        on
            sq.academic_year = gsq.academic_year
            and sq.quarter = gsq.quarter
            and sq.school_name = gsq.school_name
            and sq.grade = gsq.grade
            and sq.grade_section = gsq.grade_section
),

student_engagemnt as (
    select 
        academic_year,
        quarter,
        school_name,
        school_state,
        city,
        school_type,
        'Student Engagement' as parameters,

        case
            when student_engagement is null then 'Black'
            when student_engagement <= 49 then 'Red'
            when student_engagement >= 50 and student_engagement <= 74 then 'Amber'
            when student_engagement >= 75 then 'Green'
        end as brag
    from join_school_year
)

select * from student_engagemnt
