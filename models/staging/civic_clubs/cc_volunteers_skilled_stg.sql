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
cast("Jan__Q4" as numeric) as jan_q4,
cast("Dec___Q3" as numeric) as dec_q3,
cast("Feb___Q4" as numeric) as feb_q4,
cast("Nov___Q3" as numeric) as nov_q3,
cast("Oct___Q3" as numeric) as oct_q3,
cast("Q2_Volunteering_Hours" as numeric) as q2_volunteering_hours,
cast("Volunteering_Hours" as numeric) as volunteering_hours,
"Volunteering_type" as volunteering_type,
"Volunteering_period_" as volunteering_period


from {{ source('civic_clubs', 'Volunteer_Details') }}
