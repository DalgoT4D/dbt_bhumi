with base as (
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
        fc.quarter,
        fc.checkin_count,
        fo.odc_count
    from {{ ref('fellow_checkin_25_26') }} as fc
    left join {{ ref('fellow_odc_25_26') }} as fo
        on
            fc.fellow_id = fo.fellow_id
            and fc.quarter = fo.quarter
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
        quarter,
        'Check-in' as parameters,
        case when checkin_count is null then 1 else 0 end as black,
        case when checkin_count >= 2 then 1 else 0 end as red,
        case when checkin_count >= 3 then 1 else 0 end as amber,
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
        quarter,
        'ODC' as parameters,
        case when odc_count is null then 1 else 0 end as black,
        case when odc_count = 0 then 1 else 0 end as red,
        case when odc_count = 1 then 1 else 0 end as amber,
        case when odc_count >= 2 then 1 else 0 end as green
    from base
)

select * from checkin_brag
union all
select * from odc_brag
