with recursive date_spine as (
    select date_trunc('month', cast('2025-04-01' as date)) as month_start
    union all
    select cast((month_start + interval '1 month') as date)
    from date_spine
    where month_start < date_trunc('month', current_date)
),

month_quarter_map as (
    select
        to_char(month_start, 'Mon YYYY') as month_year,
        case
            when date_part('month', month_start) in (4, 5, 6)    then 'Apr-May-Jun'
            when date_part('month', month_start) in (7, 8, 9)    then 'Jul-Aug-Sep'
            when date_part('month', month_start) in (10, 11, 12) then 'Oct-Nov-Dec'
            when date_part('month', month_start) in (1, 2, 3)    then 'Jan-Feb-Mar'
        end as quarter
    from date_spine
),

school as (
    select
        fellow_id,
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        year_1_donor,
        year_2_donor,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section
    from {{ ref('fellow_school_25_26') }}
    group by
        school_id,
        fellow_id,
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        year_1_donor,
        year_2_donor,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section
),

school_with_quarter as (
    select
        s.*,
        m.month_year,
        m.quarter
    from school as s
    cross join month_quarter_map as m
),

class_agg as (
    select
        fellow_id,
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        year_1_donor,
        year_2_donor,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section,
        month_year,
        reporting_period,
        sum(ptms)                   as ptms_sum,
        sum(homes_visited)          as homes_visited_sum,
        sum(total_students)         as total_students_sum,
        sum(teacher_circles)        as teacher_circles_sum,
        sum(school_leader_checkins) as school_leader_checkins_sum,
        sum(helo_circles)           as helo_circles_sum,
        sum(teaching_hours)         as teaching_hours_sum
    from {{ ref('class_upd_25_26') }}
    group by
        fellow_id,
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        year_1_donor,
        year_2_donor,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section,
        month_year,
        reporting_period
)

select
    s.fellow_id,
    s.fellow_name,
    s.cohort,
    s.pm_id,
    s.pm_name,
    s.year_1_donor,
    s.year_2_donor,
    s.school_id,
    s.school_name,
    s.school_state,
    s.school_district,
    s.udise_code,
    s.school_type,
    s.grade,
    s.month_year,
    s.quarter,
    s.grade_section,
    ca.ptms_sum,
    ca.homes_visited_sum,
    ca.total_students_sum,
    ca.teacher_circles_sum,
    ca.school_leader_checkins_sum,
    ca.helo_circles_sum,
    ca.teaching_hours_sum
from school_with_quarter as s
left join class_agg as ca
    on
        s.school_id     = ca.school_id
        and s.fellow_id     = ca.fellow_id
        and s.grade         = ca.grade
        and s.grade_section = ca.grade_section
        and s.month_year    = ca.month_year
        and s.quarter       = ca.reporting_period
