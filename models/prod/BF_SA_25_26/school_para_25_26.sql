with school as (
    select distinct
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type
    from {{ ref('fellow_school_25_26') }}
),

class_agg as (
    select distinct
        school_name,
        reporting_period,
        sum(ptms) as ptms_sum,
        sum(homes_visited) as homes_visited_sum,
        sum(total_students) as total_students_sum,
        sum(teacher_circles) as teacher_circles_sum,
        sum(school_leader_checkins) as school_leader_checkins_sum
    from {{ ref('class_upd_25_26') }}
    group by school_name, reporting_period
)

select
    s.school_id,
    s.school_name,
    s.school_state,
    s.school_district,
    s.udise_code,
    s.school_type,
    ca.reporting_period,
    ca.ptms_sum,
    ca.homes_visited_sum,
    ca.total_students_sum,
    ca.teacher_circles_sum,
    ca.school_leader_checkins_sum
from school as s
left join class_agg as ca
    on s.school_name = ca.school_name
