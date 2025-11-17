-- RAG Dashboard Query for Q1 (Oct 1 - Dec 31, 2025)
-- Groups schools by classes conducted and assigns RAG status

with date_range as (
    select 
        date '2025-10-01' as start_date,
        date '2025-12-31' as end_date
),

classes_by_school as (
    select
        "School",
        count(*) as student_count,
        
        -- Count distinct class dates within date range across all module types
        round(
            (
                coalesce(count(distinct case when kitchen_garden_date between (select start_date from date_range) and (select end_date from date_range) then kitchen_garden_date end), 0) +
                coalesce(count(distinct case when waste_management_date between (select start_date from date_range) and (select end_date from date_range) then waste_management_date end), 0) +
                coalesce(count(distinct case when water_conservation_date between (select start_date from date_range) and (select end_date from date_range) then water_conservation_date end), 0) +
                coalesce(count(distinct case when climate_date between (select start_date from date_range) and (select end_date from date_range) then climate_date end), 0) +
                coalesce(count(distinct case when lifestyle_choices_date between (select start_date from date_range) and (select end_date from date_range) then lifestyle_choices_date end), 0) +
                coalesce(count(distinct case when baseline_date between (select start_date from date_range) and (select end_date from date_range) then baseline_date end), 0) +
                coalesce(count(distinct case when endline_date between (select start_date from date_range) and (select end_date from date_range) then endline_date end), 0)
            ) / 2.0 * 100
        , 2) as classes_conducted_percentage
    
    from {{ ref('combine_eco_25_26') }}
    where "School" is not null
    group by "School"
)

select
    "School",
    classes_conducted_percentage as classes_conducted,
    case 
        when classes_conducted_percentage >= 100 then 'Green'
        when classes_conducted_percentage >= 50 then 'Amber'
        when classes_conducted_percentage > 0 then 'Red'
        else 'Red'
    end as rag_status
from classes_by_school
order by classes_conducted_percentage desc, "School"
