With fellow_school As (
    Select
        fellow_id,
        fellow_name,
        cohort,
        school_id,
        school_name,
        school_state,
        school_district,
        udise_code,
        school_type,
        grade,
        grade_section,
        year_1_donor,
        year_2_donor,
        pm_id,
        pm_name,
        no_of_students
    From {{ ref('fellow_school_25_26') }}
),

fcm_data As (
    Select
        id_fcm_rating,
        fellow_id,  
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        school,
        grade,
        grade_section,
        city,
        rating,
        reporting_period,
        month_year,
        total_students,
        goal,
        cm_cohort,
        name_competency,
        Case When goal Is Not NULL And goal != 0 Then ROUND((rating::FLOAT / goal::FLOAT * 100)::NUMERIC, 2) End As "fcm%"
        -- reporting_date,
        -- cm_start_date,
        -- cm_end_date,
    From {{ ref('fcm_stg_25_26') }}

),

arg_fcm_data As (
    Select
        fellow_id,  
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        school,
        grade,
        grade_section,
        city,
        month_year,
        reporting_period,
        AVG("fcm%") As avg_fcm_percentage
    From fcm_data
    Group By
        fellow_id,  
        fellow_name,
        cohort,
        pm_id,
        pm_name,
        school,
        grade,
        grade_section,
        city,
        month_year,
        reporting_period
)

Select Distinct
    fs.fellow_id,
    fs.fellow_name,
    fs.cohort,
    fs.pm_id,
    fs.pm_name,
    fs.year_1_donor,
    fs.year_2_donor,
    fs.school_id,
    fs.school_name,
    fs.school_state,
    fs.school_district,
    fs.udise_code,
    fs.school_type,
    fs.grade,
    afd.grade_section,
    afd.month_year,
    afd.reporting_period,
    afd.avg_fcm_percentage
From fellow_school As fs
Left Join arg_fcm_data As afd
    On
        fs.fellow_id = afd.fellow_id
        And fs.grade = afd.grade
        And fs.grade_section = afd.grade_section
Where
    fs.fellow_id Is Not NULL    
