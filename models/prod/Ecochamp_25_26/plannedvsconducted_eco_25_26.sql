select
    "School"::text as school,
    "School ID"::text as school_id,
    "Donor Mapped"::text as donor_mapped,

    COUNT(distinct "Grade")::int as total_classroom,
    COUNT(distinct "Grade")::int * 7 as planned_session,
    COUNT(distinct "Grade")::int * 2 as q2_planned,
    COUNT(distinct "Grade")::int * 3 as q3_planned,
    COUNT(distinct "Grade")::int * 2 as q4_planned,

    (
        COUNT(distinct case when baseline_date is not NULL then ("Grade" || '|baseline') end)
        + COUNT(distinct case when endline_date is not NULL then ("Grade" || '|endline') end)
        + COUNT(distinct case when kitchen_garden_date is not NULL then ("Grade" || '|kitchen_garden') end)
        + COUNT(distinct case when waste_management_date is not NULL then ("Grade" || '|waste_management') end)
        + COUNT(distinct case when water_conservation_date is not NULL then ("Grade" || '|water_conservation') end)
        + COUNT(distinct case when climate_date is not NULL then ("Grade" || '|climate') end)
        + COUNT(distinct case when lifestyle_choices_date is not NULL then ("Grade" || '|lifestyle_choices') end)
    )::int as conducted_session,
    
    (
        COUNT(distinct case when baseline_date >= '2025-07-01' and baseline_date <= '2025-09-30' then ("Grade" || '|baseline') end)
        + COUNT(distinct case when endline_date >= '2025-07-01' and endline_date <= '2025-09-30' then ("Grade" || '|endline') end)
        + COUNT(distinct case when kitchen_garden_date >= '2025-07-01' and kitchen_garden_date <= '2025-09-30' then ("Grade" || '|kitchen_garden') end)
        + COUNT(distinct case when waste_management_date >= '2025-07-01' and waste_management_date <= '2025-09-30' then ("Grade" || '|waste_management') end)
        + COUNT(distinct case when water_conservation_date >= '2025-07-01' and water_conservation_date <= '2025-09-30' then ("Grade" || '|water_conservation') end)
        + COUNT(distinct case when climate_date >= '2025-07-01' and climate_date <= '2025-09-30' then ("Grade" || '|climate') end)
        + COUNT(distinct case when lifestyle_choices_date >= '2025-07-01' and lifestyle_choices_date <= '2025-09-30' then ("Grade" || '|lifestyle_choices') end)
    )::int as q2_conducted,

    (
        COUNT(distinct case when baseline_date >= '2025-10-01' and baseline_date <= '2025-12-31' then ("Grade" || '|baseline') end)
        + COUNT(distinct case when endline_date >= '2025-10-01' and endline_date <= '2025-12-31' then ("Grade" || '|endline') end)
        + COUNT(distinct case when kitchen_garden_date >= '2025-10-01' and kitchen_garden_date <= '2025-12-31' then ("Grade" || '|kitchen_garden') end)
        + COUNT(distinct case when waste_management_date >= '2025-10-01' and waste_management_date <= '2025-12-31' then ("Grade" || '|waste_management') end)
        + COUNT(distinct case when water_conservation_date >= '2025-10-01' and water_conservation_date <= '2025-12-31' then ("Grade" || '|water_conservation') end)
        + COUNT(distinct case when climate_date >= '2025-10-01' and climate_date <= '2025-12-31' then ("Grade" || '|climate') end)
        + COUNT(distinct case when lifestyle_choices_date >= '2025-10-01' and lifestyle_choices_date <= '2025-12-31' then ("Grade" || '|lifestyle_choices') end)
    )::int as q3_conducted,

    (
        COUNT(distinct case when baseline_date >= '2026-01-01' and baseline_date <= '2026-03-30' then ("Grade" || '|baseline') end)
        + COUNT(distinct case when endline_date >= '2026-01-01' and endline_date <= '2026-03-30' then ("Grade" || '|endline') end)
        + COUNT(distinct case when kitchen_garden_date >= '2026-01-01' and kitchen_garden_date <= '2026-03-30' then ("Grade" || '|kitchen_garden') end)
        + COUNT(distinct case when waste_management_date >= '2026-01-01' and waste_management_date <= '2026-03-30' then ("Grade" || '|waste_management') end)
        + COUNT(distinct case when water_conservation_date >= '2026-01-01' and water_conservation_date <= '2026-03-30' then ("Grade" || '|water_conservation') end)
        + COUNT(distinct case when climate_date >= '2026-01-01' and climate_date <= '2026-03-30' then ("Grade" || '|climate') end)
        + COUNT(distinct case when lifestyle_choices_date >= '2026-01-01' and lifestyle_choices_date <= '2026-03-30' then ("Grade" || '|lifestyle_choices') end)
    )::int as q4_conducted
    
    

from {{ ref('combine_eco_25_26') }}
where "School ID" is not NULL
group by "School", "School ID", "Donor Mapped"
