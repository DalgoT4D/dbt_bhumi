--

with params as (
    select
        "Year"::int as year,
        "Quarter"::text as quarter,
        "Quarter Start Date"::date as quarter_start,
        "Quarter End Date"::date as quarter_end,
        "Target Modules"::int as target_modules,
        "Green Percent Comp"::numeric as green_pct,
        "Amber Percent Comp"::numeric as amber_pct
    from {{ ref('RAG_quarter_params') }}
),

classes_by_school as (
    -- for each quarter param, compute per-school session counts inside that quarter
    select
        p.year,
        p.quarter,
        c."School" as school,
        count(*) filter (where c."School" is not null) as student_count,

        (
            coalesce(count(distinct case when c.kitchen_garden_date between p.quarter_start and p.quarter_end then c.kitchen_garden_date end), 0)
            + coalesce(count(distinct case when c.waste_management_date between p.quarter_start and p.quarter_end then c.waste_management_date end), 0)
            + coalesce(count(distinct case when c.water_conservation_date between p.quarter_start and p.quarter_end then c.water_conservation_date end), 0)
            + coalesce(count(distinct case when c.climate_date between p.quarter_start and p.quarter_end then c.climate_date end), 0)
            + coalesce(count(distinct case when c.lifestyle_choices_date between p.quarter_start and p.quarter_end then c.lifestyle_choices_date end), 0)
            + coalesce(count(distinct case when c.baseline_date between p.quarter_start and p.quarter_end then c.baseline_date end), 0)
            + coalesce(count(distinct case when c.endline_date between p.quarter_start and p.quarter_end then c.endline_date end), 0)
        ) as sessions_count,

        coalesce(p.target_modules, 0) as target_modules,
        p.green_pct,
        p.amber_pct

    from {{ ref('combine_eco_25_26') }} as c
    inner join params as p on true
    where c."School" is not null
    group by p.year, p.quarter, c."School", p.quarter_start, p.quarter_end, p.target_modules, p.green_pct, p.amber_pct
),

final as (
    select
        school as "School",
        year,
        quarter,
        -- avoid division by zero; if target_modules is 0 then set percentage to NULL
        case
            when target_modules > 0
                then round((sessions_count::numeric / nullif(target_modules,0) * 100)::numeric, 2)
        end as classes_completion_percentage,
        
        case
            when target_modules > 0 and round((sessions_count::numeric / nullif(target_modules,0) * 100)::numeric, 2) >= green_pct then 'Green'
            when target_modules > 0 and round((sessions_count::numeric / nullif(target_modules,0) * 100)::numeric, 2) >= amber_pct then 'Amber'
            when target_modules > 0 and round((sessions_count::numeric / nullif(target_modules,0) * 100)::numeric, 2) > 0 then 'Red'
            else 'Red'
        end as rag_status
    from classes_by_school
)

select
    year,
    quarter,
    "School",
    classes_completion_percentage as classes_conducted,
    rag_status
from final
order by year asc, quarter asc, classes_conducted desc, "School" asc
