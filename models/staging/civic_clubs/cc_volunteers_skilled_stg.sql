{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

select 
"Volunteer_Name" as volunteer_name,
"College_Name" as college_name,
"Status" as status,
"Source" as source,
"Course_Name" as course_name,
"Activities_" as activities,
CASE WHEN BTRIM("Jan__Q4"::TEXT) ~ '^[0-9.]+$' THEN "Jan__Q4"::NUMERIC END AS jan_q4,
CASE WHEN BTRIM("Dec___Q3"::TEXT) ~ '^[0-9.]+$' THEN "Dec___Q3"::NUMERIC END AS dec_q3,
CASE WHEN BTRIM("Feb___Q4"::TEXT) ~ '^[0-9.]+$' THEN "Feb___Q4"::NUMERIC END AS feb_q4,
CASE WHEN BTRIM("Nov___Q3"::TEXT) ~ '^[0-9.]+$' THEN "Nov___Q3"::NUMERIC END AS nov_q3,
CASE WHEN BTRIM("Oct___Q3"::TEXT) ~ '^[0-9.]+$' THEN "Oct___Q3"::NUMERIC END AS oct_q3,
CASE WHEN BTRIM("March___Q4"::TEXT) ~ '^[0-9.]+$' THEN "March___Q4"::NUMERIC END AS march_q4,
CASE WHEN BTRIM("Q2_Volunteering_Hours"::TEXT) ~ '^[0-9.]+$' THEN "Q2_Volunteering_Hours"::NUMERIC END AS q2_volunteering_hours,
CASE WHEN BTRIM("Volunteering_Hours"::TEXT) ~ '^[0-9.]+$' THEN "Volunteering_Hours"::NUMERIC END AS volunteering_hours,
"Volunteering_type" as volunteering_type,
"Volunteering_period_" as volunteering_period


from {{ source('civic_clubs', 'Volunteer_Details') }}
where "Volunteering_type" <> 'Pipeline'
