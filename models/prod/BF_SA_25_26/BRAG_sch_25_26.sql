with base as (

    select
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        quarter,
        homes_visited_sum,
        total_students_sum,
        ptms_sum,
        teacher_circles_sum,
        school_leader_checkins_sum
    from {{ ref('school_para_25_26') }}

),

homes_visit_brag as (

    select
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        quarter,
        'homes_visit_brag' as parameters,

        case
            when
                homes_visited_sum is null
                or total_students_sum is null
                or total_students_sum = 0
                then 1
            else 0
        end as black,

        case
            when (homes_visited_sum::float / total_students_sum::float) * 100 < 30
                then 1
            else 0
        end as red,

        case
            when (homes_visited_sum::float / total_students_sum::float) * 100 between 30 and 50
                then 1
            else 0
        end as amber,

        case
            when (homes_visited_sum::float / total_students_sum::float) * 100 > 50
                then 1
            else 0
        end as green

    from base

),

ptms_brag as (

    select
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        quarter,
        'ptms_brag' as parameters,

        case when ptms_sum is null then 1 else 0 end as black,
        case when ptms_sum = 0 then 1 else 0 end as red,
        0 as amber,
        case when ptms_sum >= 1 then 1 else 0 end as green

    from base

),

teacher_circles_brag as (

    select
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        quarter,
        'teacher_circles_brag' as parameters,

        case when teacher_circles_sum is null then 1 else 0 end as black,
        case when teacher_circles_sum = 0 then 1 else 0 end as red,
        0 as amber,
        case when teacher_circles_sum >= 1 then 1 else 0 end as green

    from base

),

school_leader_checkins_brag as (

    select
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        quarter,
        'school_leader_checkins_brag' as parameters,

        case when school_leader_checkins_sum is null then 1 else 0 end as black,
        case when school_leader_checkins_sum < 3 then 1 else 0 end as red,
        case when school_leader_checkins_sum = 3 then 1 else 0 end as amber,
        case when school_leader_checkins_sum >= 4 then 1 else 0 end as green

    from base

)

select * from homes_visit_brag
union all
select * from ptms_brag
union all
select * from teacher_circles_brag
union all
select * from school_leader_checkins_brag
