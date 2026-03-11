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

agg_fcm_data as (
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
        AVG(avg_fcm_percentage) as avg_fcm_percentage
    from {{ ref('fcm_group_25_26') }}
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
    afd.avg_fcm_percentage
from school_with_quarter as swq
left join agg_fcm_data as afd 
    on
        swq.fellow_id = afd.fellow_id 
        and swq.school_id = afd.school_id
        and swq.grade = afd.grade
        and swq.grade_section = afd.grade_section
        and swq.quarter = afd.reporting_period
        and swq.month_year = afd.month_year
