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
        'May 2025' as month_year,
        'Apr-May-Jun' as quarter
    union all
    select
        'Jun 2025' as month_year,
        'Apr-May-Jun' as quarter
    union all
    select
        'Jul 2025' as month_year,
        'Jul-Aug-Sep' as quarter
    union all
    select
        'Aug 2025' as month_year,
        'Jul-Aug-Sep' as quarter
    union all
    select
        'Sep 2025' as month_year,
        'Jul-Aug-Sep' as quarter
    union all
    select
        'Oct 2025' as month_year,
        'Oct-Nov-Dec' as quarter
    union all
    select
        'Nov 2025' as month_year,
        'Oct-Nov-Dec' as quarter
    union all
    select
        'Dec 2025' as month_year,
        'Oct-Nov-Dec' as quarter
    union all
    select
        'Jan 2026' as month_year,
        'Jan-Feb-Mar' as quarter
    union all
    select
        'Feb 2026' as month_year,
        'Jan-Feb-Mar' as quarter
    union all
    select
        'Mar 2026' as month_year,
        'Jan-Feb-Mar' as quarter
),

school_with_quarter as (
    select
        f.*,
        m.month_year,
        m.quarter
    from fellows as f
    cross join month_quarter_map as m
),

agg_odc as (
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
        AVG(student_engagement_percentage) as avg_student_engagement,
        COUNT(fellow_id) as odc_count
    from {{ ref('ODC_25_26') }}
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
    ao.total_students,
    ao.avg_student_engagement,
    ao.odc_count
from school_with_quarter as swq
left join agg_odc as ao 
    on
        swq.fellow_id = ao.fellow_id 
        and swq.school_id = ao.school_id
        and swq.grade = ao.grade
        and swq.grade_section = ao.grade_section
        and swq.quarter = ao.reporting_period
        and swq.month_year = ao.month_year
