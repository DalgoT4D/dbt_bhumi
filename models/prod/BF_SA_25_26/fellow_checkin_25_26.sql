with fellows as (
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
        f.*,
        m.month_year,
        m.quarter
    from fellows as f
    cross join month_quarter_map as m
),

agg_checkins as (
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
        SUM(total_students) as total_students,
        AVG(student_engagement) as avg_student_engagement,
        COUNT(fellow_id) as checkin_count
    from {{ ref('checkins_25_26') }}
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
    swq.fellow_id,
    swq.fellow_name,
    swq.cohort,
    swq.pm_id,
    swq.pm_name,
    swq.year_1_donor,
    swq.year_2_donor,
    swq.school_id,
    swq.school_name,
    swq.school_state,
    swq.school_district,
    swq.udise_code,
    swq.school_type,
    swq.grade,
    swq.month_year,
    swq.quarter,
    swq.grade_section,
    ac.total_students,
    ac.avg_student_engagement,
    ac.checkin_count
from school_with_quarter as swq
left join agg_checkins as ac 
    on
        swq.fellow_id = ac.fellow_id 
        and swq.school_id = ac.school_id
        and swq.grade = ac.grade
        and swq.grade_section = ac.grade_section
        and swq.quarter = ac.reporting_period
        and swq.month_year = ac.month_year
