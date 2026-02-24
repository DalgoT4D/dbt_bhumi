select
    school_id,
    school_name,
    school_state,
    school_district,
    udise_code,
    school_type,
    reporting_period,
    case
        when homes_visited_sum is NULL or total_students_sum is NULL or total_students_sum = 0 then 'Black'
        when (homes_visited_sum::float / total_students_sum::float) * 100 < 30 then 'Red'
        when (homes_visited_sum::float / total_students_sum::float) * 100 >= 30 and (homes_visited_sum::float / total_students_sum::float) * 100 <= 50 then 'Amber'
        when (homes_visited_sum::float / total_students_sum::float) * 100 > 50 then 'Green'
    end as homes_visit_brag,
    case
        when ptms_sum is NULL then 'Black'
        when ptms_sum = 0 then 'Red'
        when ptms_sum >= 1 then 'Green'
    end as ptms_brag,
    case
        when teacher_circles_sum is NULL then 'Black'
        when teacher_circles_sum = 0 then 'Red'
        when teacher_circles_sum >= 1 then 'Green'
    end as teacher_circles_brag,
    case
        when school_leader_checkins_sum is NULL then 'Black'
        when school_leader_checkins_sum < 3 then 'Red'
        when school_leader_checkins_sum = 3 then 'Amber'
        when school_leader_checkins_sum >= 4 then 'Green'
    end as school_leader_checkins_brag
from {{ ref('school_para_25_26') }}
