
select
    "School"::text as school,
    "School ID"::text as school_id,
    "Donor Mapped"::text as donor_mapped,

    COUNT(DISTINCT "Grade")::int as total_classroom,
    COUNT(DISTINCT "Grade")::int * 7 as planned_session,
    COUNT(DISTINCT "Grade")::int * 2 as q2_planned,
    COUNT(DISTINCT "Grade")::int * 3 as q3_planned,
    COUNT(DISTINCT "Grade")::int * 2 as q4_planned,

    (
        COUNT(DISTINCT CASE WHEN baseline_date IS NOT NULL THEN ("Grade" || '|baseline') END)
        + COUNT(DISTINCT CASE WHEN endline_date IS NOT NULL THEN ("Grade" || '|endline') END)
        + COUNT(DISTINCT CASE WHEN kitchen_garden_date IS NOT NULL THEN ("Grade" || '|kitchen_garden') END)
        + COUNT(DISTINCT CASE WHEN waste_management_date IS NOT NULL THEN ("Grade" || '|waste_management') END)
        + COUNT(DISTINCT CASE WHEN water_conservation_date IS NOT NULL THEN ("Grade" || '|water_conservation') END)
        + COUNT(DISTINCT CASE WHEN climate_date IS NOT NULL THEN ("Grade" || '|climate') END)
        + COUNT(DISTINCT CASE WHEN lifestyle_choices_date IS NOT NULL THEN ("Grade" || '|lifestyle_choices') END)
    )::int as conducted_session,
    
        (
                COUNT(DISTINCT CASE WHEN baseline_date >= '2025-07-01' AND baseline_date <= '2025-09-30' THEN ("Grade" || '|baseline') END)
            + COUNT(DISTINCT CASE WHEN endline_date >= '2025-07-01' AND endline_date <= '2025-09-30' THEN ("Grade" || '|endline') END)
            + COUNT(DISTINCT CASE WHEN kitchen_garden_date >= '2025-07-01' AND kitchen_garden_date <= '2025-09-30' THEN ("Grade" || '|kitchen_garden') END)
            + COUNT(DISTINCT CASE WHEN waste_management_date >= '2025-07-01' AND waste_management_date <= '2025-09-30' THEN ("Grade" || '|waste_management') END)
            + COUNT(DISTINCT CASE WHEN water_conservation_date >= '2025-07-01' AND water_conservation_date <= '2025-09-30' THEN ("Grade" || '|water_conservation') END)
            + COUNT(DISTINCT CASE WHEN climate_date >= '2025-07-01' AND climate_date <= '2025-09-30' THEN ("Grade" || '|climate') END)
            + COUNT(DISTINCT CASE WHEN lifestyle_choices_date >= '2025-07-01' AND lifestyle_choices_date <= '2025-09-30' THEN ("Grade" || '|lifestyle_choices') END)
        )::int as q2_conducted,

        (
                COUNT(DISTINCT CASE WHEN baseline_date >= '2025-10-01' AND baseline_date <= '2025-12-31' THEN ("Grade" || '|baseline') END)
            + COUNT(DISTINCT CASE WHEN endline_date >= '2025-10-01' AND endline_date <= '2025-12-31' THEN ("Grade" || '|endline') END)
            + COUNT(DISTINCT CASE WHEN kitchen_garden_date >= '2025-10-01' AND kitchen_garden_date <= '2025-12-31' THEN ("Grade" || '|kitchen_garden') END)
            + COUNT(DISTINCT CASE WHEN waste_management_date >= '2025-10-01' AND waste_management_date <= '2025-12-31' THEN ("Grade" || '|waste_management') END)
            + COUNT(DISTINCT CASE WHEN water_conservation_date >= '2025-10-01' AND water_conservation_date <= '2025-12-31' THEN ("Grade" || '|water_conservation') END)
            + COUNT(DISTINCT CASE WHEN climate_date >= '2025-10-01' AND climate_date <= '2025-12-31' THEN ("Grade" || '|climate') END)
            + COUNT(DISTINCT CASE WHEN lifestyle_choices_date >= '2025-10-01' AND lifestyle_choices_date <= '2025-12-31' THEN ("Grade" || '|lifestyle_choices') END)
        )::int as q3_conducted,

        (
                COUNT(DISTINCT CASE WHEN baseline_date >= '2026-01-01' AND baseline_date <= '2026-03-30' THEN ("Grade" || '|baseline') END)
            + COUNT(DISTINCT CASE WHEN endline_date >= '2026-01-01' AND endline_date <= '2026-03-30' THEN ("Grade" || '|endline') END)
            + COUNT(DISTINCT CASE WHEN kitchen_garden_date >= '2026-01-01' AND kitchen_garden_date <= '2026-03-30' THEN ("Grade" || '|kitchen_garden') END)
            + COUNT(DISTINCT CASE WHEN waste_management_date >= '2026-01-01' AND waste_management_date <= '2026-03-30' THEN ("Grade" || '|waste_management') END)
            + COUNT(DISTINCT CASE WHEN water_conservation_date >= '2026-01-01' AND water_conservation_date <= '2026-03-30' THEN ("Grade" || '|water_conservation') END)
            + COUNT(DISTINCT CASE WHEN climate_date >= '2026-01-01' AND climate_date <= '2026-03-30' THEN ("Grade" || '|climate') END)
            + COUNT(DISTINCT CASE WHEN lifestyle_choices_date >= '2026-01-01' AND lifestyle_choices_date <= '2026-03-30' THEN ("Grade" || '|lifestyle_choices') END)
        )::int as q4_conducted
    
    

    from {{ ref('combine_eco_25_26') }}
where "School ID" is not null
group by "School", "School ID", "Donor Mapped"
