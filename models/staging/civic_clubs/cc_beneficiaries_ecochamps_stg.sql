{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

select
"School_ID" as school_id,
"School" as school,
"Chapter" as chapter,
"Grade" as grade,
"Mo" as month,
"Climate" as climate,
"Endline" as endline,
"Baseline" as baseline,
"Donor_Mapped" as donor_mapped,
case when btrim("Student_Count"::text) ~ '^\d+$' then btrim("Student_Count"::text)::integer end as student_count,
case when btrim("Total_Students_Count"::text) ~ '^\d+$' then btrim("Total_Students_Count"::text)::integer end as total_students_count,
"Kitchen_Garden" as kitchen_garden,
"Waste_Management" as waste_management,
"Water_Conservation" as water_conservation,
"Lifestyle___Choices" as lifestyle_choices,
case when btrim("Classroom_Attendance"::text) ~ '^\d+$' then btrim("Classroom_Attendance"::text)::integer end as classroom_attendance,
"Center_Coordiantor__1_" as center_coordinator_1,
"Center_Coordianator__2_" as center_coordinator_2,
"Session_Completion_Status" as session_completion_status,
"Progress_Bar__Q2___Target___2_Modules_" as progress_bar_q2

from {{ source('ecochamps25_26', 'Data_Tracker') }}
where "School_ID" is not null and btrim("School_ID") != ''
and "School" is not null and btrim("School") != ''
