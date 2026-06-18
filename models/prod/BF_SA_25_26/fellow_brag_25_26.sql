{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

with recursive date_spine as (
    -- Generates one row per month from Apr 2025 through current month.
    -- Change the end condition to a fixed date (e.g. '2026-03-31') 
    -- if you want a fixed academic year boundary instead of "up to today".
    select date_trunc('month', cast('2025-04-01' as date)) as month_start
    union all
    select month_start + interval '1 month'
    from date_spine
    where month_start < date_trunc('month', current_date)
),

month_quarter_map as (
    select
        -- to_char(month_start, 'Mon YYYY') as month_year,
        case
            when date_part('month', month_start) in (4, 5, 6)  then 'Apr-May-Jun'
            when date_part('month', month_start) in (7, 8, 9)  then 'Jul-Aug-Sep'
            when date_part('month', month_start) in (10, 11, 12) then 'Oct-Nov-Dec'
            when date_part('month', month_start) in (1, 2, 3)  then 'Jan-Feb-Mar'
        end as quarter
    from date_spine
),

grouped_checkins as (
    select
        fellow_id,
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        donor_id,
        donor_name,
        academic_year,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section,
        quarter,
        sum(checkin_count) as checkin_count
    from {{ ref('fellow_checkin_25_26') }}
    group by
        fellow_id,
        fellow_name,
        cohort,        
        pm_id,
        pm_name,
        donor_id,
        donor_name,
        academic_year,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section,
        quarter
),

grouped_odc as (
    select
        fellow_id,
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        donor_id,
        donor_name,
        academic_year,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section,
        quarter,
        sum(odc_count) as odc_count 
    from {{ ref('fellow_odc_25_26') }}
    group by
        fellow_id,
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        donor_id,
        donor_name,
        academic_year,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section,
        quarter
),

grouped_fcm as (
    select
        fellow_id,
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        donor_id,
        donor_name,
        academic_year,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section,
        quarter,
        avg(avg_fcm_percentage) as avg_fcm_percentage
    from {{ ref('fcm_agg_25_26') }}
    group by
        fellow_id,
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        donor_id,
        donor_name,
        academic_year,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section,
        quarter
),

fellows_school as (
    select
        fellow_id,
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        donor_id,
        donor_name,
        academic_year,
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
        donor_id,
        donor_name,
        academic_year,
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
        fs.*,
        m.quarter
    from fellows_school as fs
    cross join month_quarter_map as m
),

base as (
    select
        swq.fellow_id,
        swq.fellow_name,
        swq.cohort,
        swq.pm_id,
        swq.pm_name,
        swq.donor_id,
        swq.donor_name,
        swq.school_id,
        swq.school_name,
        swq.school_state,
        swq.school_district,
        swq.udise_code,
        swq.school_type,
        swq.grade,
        swq.grade_section,
        swq.quarter,
        swq.academic_year,
        fc.checkin_count,
        fo.odc_count,
        ff.avg_fcm_percentage
    from school_with_quarter as swq
    left join grouped_checkins as fc
        on
            swq.fellow_id = fc.fellow_id
            and swq.school_id = fc.school_id
            and swq.grade = fc.grade
            and swq.grade_section = fc.grade_section
            and swq.quarter = fc.quarter
            and swq.academic_year = fc.academic_year
    left join grouped_odc as fo
        on
            swq.fellow_id = fo.fellow_id
            and swq.school_id = fo.school_id
            and swq.grade = fo.grade
            and swq.grade_section = fo.grade_section
            and swq.quarter = fo.quarter
            and swq.academic_year = fo.academic_year
    left join grouped_fcm as ff
        on
            swq.fellow_id = ff.fellow_id
            and swq.school_id = ff.school_id
            and swq.grade = ff.grade
            and swq.grade_section = ff.grade_section
            and swq.quarter = ff.quarter
            and swq.academic_year = ff.academic_year
),

checkin_brag as (
    select
        fellow_id,
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        donor_id,
        donor_name,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section,
        quarter,
        academic_year,
        'Check-in' as parameters,
        case when checkin_count is null then 1 else 0 end as black,
        case when checkin_count <= 2 then 1 else 0 end as red,
        case when checkin_count >= 3 and checkin_count <= 5 then 1 else 0 end as amber,
        case when checkin_count >= 6 then 1 else 0 end as green
    from base
),

odc_brag as (
    select
        fellow_id,
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        donor_id,
        donor_name,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section,
        quarter,
        academic_year,
        'ODC' as parameters,
        case when odc_count is null then 1 else 0 end as black,
        case when odc_count = 0 then 1 else 0 end as red,
        case when odc_count = 1 then 1 else 0 end as amber,
        case when odc_count >= 2 then 1 else 0 end as green
    from base
),

fcm_brag as (
    select
        fellow_id,
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        donor_id,
        donor_name,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section,
        quarter,
        academic_year,
        'FCM%' as parameters,
        case when avg_fcm_percentage is null then 1 else 0 end as black,
        case when avg_fcm_percentage <= 49 then 1 else 0 end as red,
        case when avg_fcm_percentage >= 50 and avg_fcm_percentage <= 74 then 1 else 0 end as amber,
        case when avg_fcm_percentage >= 75 then 1 else 0 end as green
    from base
)

select * from checkin_brag
union all
select * from odc_brag
union all
select * from fcm_brag
