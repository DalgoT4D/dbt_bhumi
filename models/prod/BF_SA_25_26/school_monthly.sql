{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

with class_upd_int as (
    select 
        academic_year,
        month,
        school_name,
        school_state,
        city,
        school_type,
        sum(ptms) as ptms,
        -- sum(helo_circles) as helo_circles,
        round(avg(homes_visited)) as homes_visited,
        -- sum(teaching_hours) as teaching_hours,
        sum(teacher_circles) as teacher_circles,
        sum(school_leader_checkins) as school_leader_checkins
    from {{ ref('class_upd_int') }}
    group by
        academic_year,
        month,
        school_name,
        school_state,
        city,
        school_type
),

ptms_para as (
    select 
        academic_year,
        month,
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
    from class_upd_int
),

home_visits_para as (
    select 
        academic_year,
        month,
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
    from class_upd_int
),

school_led_checkins_para as (
    select 
        academic_year,
        month,
        school_name,
        school_state,
        city,
        school_type,
        'School Leader Check-ins' as parameters,

        case
            when school_leader_checkins is null then 'Black'
            when school_leader_checkins < 3 then 'Red'
            when school_leader_checkins = 3 then 'Amber'
            when school_leader_checkins >= 4 then 'Green'
        end as brag
    from class_upd_int
),

teacher_circles_para as (
    select 
        academic_year,
        month,
        school_name,
        school_state,
        city,
        school_type,
        'Teacher Circles' as parameters,

        case
            when teacher_circles is null then 'Black'
            when teacher_circles = 0 then 'Red'
            when teacher_circles >= 1 then 'Green'
        end as brag
    from class_upd_int
)

select * from ptms_para
union all
select * from home_visits_para
union all
select * from school_led_checkins_para
union all
select * from teacher_circles_para
