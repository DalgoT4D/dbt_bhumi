with school as (
    select
        school_id,
        max(school_name) as school_name,
        max(school_state) as school_state,
        max(school_district) as school_district,
        max(udise_code) as udise_code,
        max(school_type) as school_type
    from {{ ref('fellow_school_25_26') }}
    group by school_id
),

quarters as (
    select 'Apr-May-Jun' as quarter
    union all
    select 'Jul-Aug-Sep'
    union all
    select 'Oct-Nov-Dec'
    union all
    select 'Jan-Feb-Mar'
),

school_with_quarter as (
    select
        s.*,
        q.quarter
    from school as s
    cross join quarters as q
),

class_agg as (
    select
        school_id,
        reporting_period,
        sum(ptms) as ptms_sum,
        sum(homes_visited) as homes_visited_sum,
        sum(total_students) as total_students_sum,
        sum(teacher_circles) as teacher_circles_sum,
        sum(school_leader_checkins) as school_leader_checkins_sum
    from {{ ref('class_upd_25_26') }}
    group by
        school_id,
        reporting_period
)

select
    s.school_id,
    s.school_name,
    s.school_state,
    s.school_district,
    s.udise_code,
    s.school_type,
    s.quarter,
    ca.ptms_sum,
    ca.homes_visited_sum,
    ca.total_students_sum,
    ca.teacher_circles_sum,
    ca.school_leader_checkins_sum
from school_with_quarter as s
left join class_agg as ca
    on
        s.school_id = ca.school_id
        and s.quarter = ca.reporting_period
