-- RAG Dashboard Query for Overall Period
-- Groups schools by classes conducted and assigns RAG status

with classes_by_school as (
    select
        "School",
        count(*) as student_count,
        
        -- Count distinct class dates within date range across all module types
        ROUND(
                (
                    (
                        COALESCE(COUNT(DISTINCT kitchen_garden_date), 0)
                        + COALESCE(COUNT(DISTINCT waste_management_date), 0)
                        + COALESCE(COUNT(DISTINCT water_conservation_date), 0)
                        + COALESCE(COUNT(DISTINCT climate_date), 0)
                        + COALESCE(COUNT(DISTINCT lifestyle_choices_date), 0)
                        + COALESCE(COUNT(DISTINCT baseline_date), 0)
                        + COALESCE(COUNT(DISTINCT endline_date), 0)
                    ) / 7.0
                ) * 100,
            2) as classes_conducted_percentage
    
    from {{ ref('combine_eco_25_26') }}
    where "School" is not null
    group by "School"
)

select
    "School",
    classes_conducted_percentage as classes_conducted,
    case 
        when classes_conducted_percentage >= 80 then 'Green'
        when classes_conducted_percentage >= 66 then 'Amber'
        when classes_conducted_percentage > 0 then 'Red'
        else 'Red'
    end as rag_status
from classes_by_school
order by classes_conducted_percentage desc, "School"
