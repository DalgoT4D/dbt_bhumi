select
    school_id,
    school_name,
    school_state,
    school_district,
    udise_code,
    school_type,
    reporting_period,
    parameters,
    Black,
    Red,
    Amber,
    Green
from (
    select
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        reporting_period,
        'homes_visit_brag' as parameters,
        case when homes_visited_sum is NULL or total_students_sum is NULL or total_students_sum = 0 then 1 else 0 end as Black,
        case when (homes_visited_sum::float / total_students_sum::float) * 100 < 30 then 1 else 0 end as Red,
        case when (homes_visited_sum::float / total_students_sum::float) * 100 >= 30 and (homes_visited_sum::float / total_students_sum::float) * 100 <= 50 then 1 else 0 end as Amber,
        case when (homes_visited_sum::float / total_students_sum::float) * 100 > 50 then 1 else 0 end as Green
    from {{ ref('school_para_25_26') }}
    union all
    select
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        reporting_period,
        'ptms_brag' as parameters,
        case when ptms_sum is NULL then 1 else 0 end as Black,
        case when ptms_sum = 0 then 1 else 0 end as Red,
        0 as Amber,
        case when ptms_sum >= 1 then 1 else 0 end as Green
    from {{ ref('school_para_25_26') }}
    union all
    select
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        reporting_period,
        'teacher_circles_brag' as parameters,
        case when teacher_circles_sum is NULL then 1 else 0 end as Black,
        case when teacher_circles_sum = 0 then 1 else 0 end as Red,
        0 as Amber,
        case when teacher_circles_sum >= 1 then 1 else 0 end as Green
    from {{ ref('school_para_25_26') }}
    union all
    select
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        reporting_period,
        'school_leader_checkins_brag' as parameters,
        case when school_leader_checkins_sum is NULL then 1 else 0 end as Black,
        case when school_leader_checkins_sum < 3 then 1 else 0 end as Red,
        case when school_leader_checkins_sum = 3 then 1 else 0 end as Amber,
        case when school_leader_checkins_sum >= 4 then 1 else 0 end as Green
    from {{ ref('school_para_25_26') }}
)
