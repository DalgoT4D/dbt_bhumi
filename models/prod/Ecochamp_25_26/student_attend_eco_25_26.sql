-- Student Attendance RAG Dashboard
-- Groups by School and Student Name, calculates attendance percentage and assigns RAG status

with student_attendance as (
    select
        "School",
        
        -- Count distinct class dates across all module/assessment types
        round(
            (
                coalesce(count(distinct case when kitchen_garden_date is not null then kitchen_garden_date end), 0) +
                coalesce(count(distinct case when waste_management_date is not null then waste_management_date end), 0) +
                coalesce(count(distinct case when water_conservation_date is not null then water_conservation_date end), 0) +
                coalesce(count(distinct case when climate_date is not null then climate_date end), 0) +
                coalesce(count(distinct case when lifestyle_choices_date is not null then lifestyle_choices_date end), 0) +
                coalesce(count(distinct case when baseline_date is not null then baseline_date end), 0) +
                coalesce(count(distinct case when endline_date is not null then endline_date end), 0)
            ) / 7.0 * 100
        , 2) as attendance_percentage
    
    from {{ ref('combine_eco_25_26') }}
    where "School" is not null
    group by "School"
)

select
    "School",
    attendance_percentage as student_attendance,
    case 
        when attendance_percentage >= 80 then 'Green'
        when attendance_percentage >= 66 then 'Amber'
        when attendance_percentage > 0 then 'Red'
        else 'Red'
    end as rag_status
from student_attendance
order by attendance_percentage desc, "School"
