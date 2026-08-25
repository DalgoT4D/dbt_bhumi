{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

with school_data as (
    select distinct
        academic_year,
        school_name,
        school_state,
        city,
        school_type
    from {{ ref('fellow_scl_cls_data') }}
    where
        school_name is not null
),

-- select * from school_data

calender_year as (
    select distinct quarter
    from {{ ref('calender_year') }}
),

school_quarter as (
    select
        sd.academic_year,
        cy.quarter,
        sd.school_name,
        sd.school_state,
        sd.city,
        sd.school_type
    from school_data as sd
    cross join calender_year as cy
),

-- select * from school_quarter

class_upd_int as (
    select distinct
        academic_year,
        school_name,
        school_state,
        city,
        school_type,
        quarter,
        sum(ptms) as ptms,
        -- sum(helo_circles) as helo_circles,
        avg(homes_visited) as homes_visited,
        sum(teaching_hours) as teaching_hours,
        sum(teacher_circles) as teacher_circles,
        sum(school_leader_checkins) as school_leader_checkins
    from {{ ref('class_upd_int') }}
    group by
        academic_year,
        school_name,
        school_state,
        city,
        school_type,
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

        gsq.ptms,
        -- gsq.helo_circles,
        gsq.homes_visited,
        gsq.teaching_hours,
        gsq.teacher_circles,
        gsq.school_leader_checkins
    from school_quarter as sq
    left join class_upd_int as gsq
        on
            sq.academic_year = gsq.academic_year
            and sq.school_name = gsq.school_name
            and sq.quarter = gsq.quarter

),

ptms_para as (
    select 
        academic_year,
        quarter,
        school_name,
        school_state,
        city,
        school_type,
        'PTMS' as parameters,

        case
            when ptms is null then 'Black'
            when ptms = 0 then 'Red'
            when ptms >= 1 then 'Green'
        end as brag
    from join_school_year
),

home_visits_para as (
    select 
        academic_year,
        quarter,
        school_name,
        school_state,
        city,
        school_type,
        'Home visits' as parameters,

        case
            when homes_visited is null then 'Black'
            when homes_visited < 30 then 'Red'
            when homes_visited >= 30 and homes_visited <=50 then 'Amber'
            when homes_visited > 50 then 'Green'
        end as brag
    from join_school_year
),

school_led_checkins_para as (
    select 
        academic_year,
        quarter,
        school_name,
        school_state,
        city,
        school_type,
        'School Leader Check-ins' as parameters,

        case
            when homes_visited is null then 'Black'
            when homes_visited < 3 then 'Red'
            when homes_visited = 3 then 'Amber'
            when homes_visited >= 4 then 'Green'
        end as brag
    from join_school_year
),

teacher_circles_para as (
    select 
        academic_year,
        quarter,
        school_name,
        school_state,
        city,
        school_type,
        'Teacher Circles' as parameters,

        case
            when ptms is null then 'Black'
            when ptms = 0 then 'Red'
            when ptms >= 1 then 'Green'
        end as brag
    from join_school_year
)

select * from ptms_para
union all
select * from home_visits_para
union all
select * from school_led_checkins_para
union all
select * from teacher_circles_para
