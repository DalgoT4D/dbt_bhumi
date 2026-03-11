with grouped_checkins as (
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
        quarter,
        SUM(checkin_count) as checkin_count
    from {{ ref('fellow_checkin_25_26') }}
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
        quarter
),

grouped_odc as (
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
        quarter,
        SUM(odc_count) as odc_count 
    from {{ ref('fellow_odc_25_26') }}
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
        quarter
),

grouped_fcm as (
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
        quarter,
        AVG(avg_fcm_percentage) as avg_fcm_percentage
    from {{ ref('fcm_agg_25_26') }}
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
        quarter
),

base as (
    select
        fc.fellow_id,
        fc.fellow_name,
        fc.cohort,
        fc.pm_id,
        fc.pm_name,
        fc.year_1_donor,
        fc.year_2_donor,
        fc.school_id,
        fc.school_name,
        fc.school_state,
        fc.school_district,
        fc.udise_code,
        fc.school_type,
        fc.grade,
        fc.grade_section,
        fc.quarter,
        fc.checkin_count,
        fo.odc_count,
        ff.avg_fcm_percentage
    from grouped_checkins as fc
    inner join grouped_odc as fo
        on
            fc.fellow_id = fo.fellow_id
            and fc.quarter = fo.quarter
    inner join grouped_fcm as ff
        on
            fc.fellow_id = ff.fellow_id
            and fc.quarter = ff.quarter
),

checkin_brag as (
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
        quarter,
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
        quarter,
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
        quarter,
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
