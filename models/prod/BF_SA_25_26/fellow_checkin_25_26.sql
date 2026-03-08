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
        school_type
    from {{ ref('fellow_school_25_26') }}
    group by fellow_id, fellow_name, cohort, pm_id, pm_name, year_1_donor, year_2_donor, school_id, school_name, school_state, school_district, udise_code, school_type
),

quarters as (
    select 'Apr-May-Jun' as quarter
    union all
    select 'Jul-Aug-Sep'
    union all
    select 'Oct-Nov-Dec'
    union all
    select 'Jan-Feb-Mar'
),

school_with_quarter as (
    select
        f.*,
        q.quarter
    from fellows as f
    cross join quarters as q
),

agg_checkins as (
    SELECT
	    fellow_id,
	    fellow_name,
	    cohort,
	    year_1_donor,
	    year_2_donor,
        reporting_period,
	    SUM(total_students) AS total_students,
	    AVG(student_engagement) AS avg_student_engagement,
	    SUM(no_of_students) AS no_of_students,
	    COUNT(fellow_id) AS checkin_count
FROM {{ ref('checkins_25_26') }}
GROUP BY
	fellow_id,
	fellow_name,
	cohort,
	year_1_donor,
	year_2_donor,
    reporting_period
)

SELECT
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
    swq.quarter,
    ac.total_students,
    ac.avg_student_engagement,
    ac.no_of_students,
    ac.checkin_count
FROM school_with_quarter AS swq
LEFT JOIN agg_checkins AS ac 
    ON swq.fellow_id = ac.fellow_id 
    AND swq.quarter = ac.reporting_period
