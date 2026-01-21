
select
    "School"::text as school,
    "School ID"::text as school_id,
    "Donor Mapped"::text as donor_mapped,
    "Quarter"::text as quarter,
    COUNT(DISTINCT "Grade")::int as total_classroom,
    COUNT(DISTINCT "Grade")::int * 7 as planned_session,
    COUNT(DISTINCT "Grade")::int * 2 as q2_planned,
    COUNT(DISTINCT "Grade")::int * 3 as q3_planned,
    COUNT(DISTINCT "Grade")::int * 2 as q4_planned
    
    

    from {{ ref('eco_student25_26_stg') }}
where "School" is not null
group by "School", "School ID", "Donor Mapped", "Quarter"
