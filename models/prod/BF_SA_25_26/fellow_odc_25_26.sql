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

fellows as (
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
        sum(total_students)                    as total_students,
        avg(student_engagement_percentage)     as avg_student_engagement,
        count(fellow_id)                       as odc_count
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
        swq.fellow_id     = ao.fellow_id
        and swq.school_id     = ao.school_id
        and swq.grade         = ao.grade
        and swq.grade_section = ao.grade_section
        and swq.quarter       = ao.reporting_period
        and swq.month_year    = ao.month_year
