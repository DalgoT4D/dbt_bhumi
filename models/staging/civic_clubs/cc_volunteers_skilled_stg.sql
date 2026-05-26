{{ config(
  materialized='table',
  tags=["civic_clubs", "staging"]
) }}

select 
    "Volunteer_Name" as volunteer_name,
    "College_Name" as college_name,
    "Status" as status,
    "Source" as source,
    "Course_Name" as course_name,
    "Activities_" as activities,
    case when BTRIM("Jan__Q4"::TEXT) ~ '^[0-9.]+$' then "Jan__Q4"::NUMERIC end as jan_q4,
    case when BTRIM("Dec___Q3"::TEXT) ~ '^[0-9.]+$' then "Dec___Q3"::NUMERIC end as dec_q3,
    case when BTRIM("Feb___Q4"::TEXT) ~ '^[0-9.]+$' then "Feb___Q4"::NUMERIC end as feb_q4,
    case when BTRIM("Nov___Q3"::TEXT) ~ '^[0-9.]+$' then "Nov___Q3"::NUMERIC end as nov_q3,
    case when BTRIM("Oct___Q3"::TEXT) ~ '^[0-9.]+$' then "Oct___Q3"::NUMERIC end as oct_q3,
    case when BTRIM("March___Q4"::TEXT) ~ '^[0-9.]+$' then "March___Q4"::NUMERIC end as march_q4,
    case when BTRIM("Q2_Volunteering_Hours"::TEXT) ~ '^[0-9.]+$' then "Q2_Volunteering_Hours"::NUMERIC end as q2_volunteering_hours,
    case when BTRIM("Volunteering_Hours"::TEXT) ~ '^[0-9.]+$' then "Volunteering_Hours"::NUMERIC end as volunteering_hours,
    "Volunteering_type" as volunteering_type,
    "Volunteering_period_" as volunteering_period

from {{ source('civic_clubs', 'Volunteer_Details') }}
where "Volunteering_type" <> 'Pipeline'
