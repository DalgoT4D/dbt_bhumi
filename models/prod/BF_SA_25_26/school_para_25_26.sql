with school as (
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
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section
),

month_quarter_map as (
    select
        'Apr 2025' as month_year,
        'Apr-May-Jun' as quarter
    union all
    select
        'May 2025',
        'Apr-May-Jun'
    union all
    select
        'Jun 2025',
        'Apr-May-Jun'
    union all
    select
        'Jul 2025',
        'Jul-Aug-Sep'
    union all
    select
        'Aug 2025',
        'Jul-Aug-Sep'
    union all
    select
        'Sep 2025',
        'Jul-Aug-Sep'
    union all
    select
        'Oct 2025',
        'Oct-Nov-Dec'
    union all
    select
        'Nov 2025',
        'Oct-Nov-Dec'
    union all
    select
        'Dec 2025',
        'Oct-Nov-Dec'
    union all
    select
        'Jan 2026',
        'Jan-Feb-Mar'
    union all
    select
        'Feb 2026',
        'Jan-Feb-Mar'
    union all
    select
        'Mar 2026',
        'Jan-Feb-Mar'
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
        sum(ptms) as ptms_sum,
        sum(homes_visited) as homes_visited_sum,
        sum(total_students) as total_students_sum,
        sum(teacher_circles) as teacher_circles_sum,
        sum(school_leader_checkins) as school_leader_checkins_sum,
        sum(helo_circles) as helo_circles_sum,
        sum(teaching_hours) as teaching_hours_sum
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
        s.school_id = ca.school_id
        and s.fellow_id = ca.fellow_id
        and s.grade = ca.grade
        and s.grade_section = ca.grade_section
        and s.month_year = ca.month_year
        and s.quarter = ca.reporting_period
