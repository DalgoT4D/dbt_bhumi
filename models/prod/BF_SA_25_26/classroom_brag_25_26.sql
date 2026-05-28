with class_updates as (

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
        sum(teaching_hours_sum) as teaching_hours_sum,
        sum(helo_circles_sum) as helo_circles_sum
    from {{ ref('school_para_25_26') }}
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
        avg(avg_student_engagement) as avg_student_engagement
    from {{ ref('fellow_odc_25_26') }}
    group by
        fellow_id,
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
        cu.fellow_id,
        cu.fellow_name,
        cu.cohort,
        cu.pm_id,
        cu.pm_name,
        cu.year_1_donor,
        cu.year_2_donor,
        cu.school_id,
        cu.school_name,
        cu.school_state,
        cu.school_district,
        cu.udise_code,
        cu.school_type,
        cu.grade,
        cu.grade_section,
        cu.quarter,
        cu.teaching_hours_sum,
        cu.helo_circles_sum,
        fo.avg_student_engagement

    from class_updates as cu
    inner join grouped_odc as fo
        on
            cu.fellow_id = fo.fellow_id
            and cu.quarter = fo.quarter
),

teaching_brag as (
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
        'Teaching Hours' as parameters,
        case when teaching_hours_sum is null then 1 else 0 end as black,
        case when teaching_hours_sum <= 108 then 1 else 0 end as red,
        case when teaching_hours_sum >= 120 and teaching_hours_sum < 156  then 1 else 0 end as amber,
        case when teaching_hours_sum >= 156 then 1 else 0 end as green
    from base
),

helo_brag as (
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
        'Helo Circles' as parameters,
        case when helo_circles_sum is null then 1 else 0 end as black,
        case when helo_circles_sum >= 0 and helo_circles_sum <=8 then 1 else 0 end as red,
        case when helo_circles_sum >= 9 and helo_circles_sum <= 16 then 1 else 0 end as amber,
        case when helo_circles_sum >= 17 then 1 else 0 end as green
    from base
),

engage_brag as (
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
        'Student Engagement' as parameters,
        case when avg_student_engagement is null then 1 else 0 end as black,
        case when avg_student_engagement <= 49 then 1 else 0 end as red,
        case when avg_student_engagement >= 50 and avg_student_engagement <= 74 then 1 else 0 end as amber,
        case when avg_student_engagement >= 75 then 1 else 0 end as green
    from base
)

select * from teaching_brag
union all
select * from helo_brag
union all
select * from engage_brag
