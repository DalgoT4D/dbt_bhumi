select
    class,
    donor,
    state,
    district,
    school_name,
    trainer_name,

    -- average total marks
    round(avg(total_marks_base), 2) as avg_total_marks_base,
    round(avg(total_marks_end), 2) as avg_total_marks_end,

    -- average subject scores - baseline
    round(avg(maths_base), 2) as avg_maths_base,
    round(avg(tech_base), 2) as avg_tech_base,
    round(avg(science_base), 2) as avg_science_base,
    round(avg(knowledge_base), 2) as avg_knowledge_base,
    round(avg(application_base), 2) as avg_application_base,
    round(avg(understanding_base), 2) as avg_understanding_base,

    -- average subject scores - endline
    round(avg(maths_end), 2) as avg_maths_end,
    round(avg(tech_end), 2) as avg_tech_end,
    round(avg(science_end), 2) as avg_science_end,
    round(avg(knowledge_end), 2) as avg_knowledge_end,
    round(avg(application_end), 2) as avg_application_end,
    round(avg(understanding_end), 2) as avg_understanding_end,

    -- scored_base % buckets (student counts per group)
    count(case when scored_base between 0 and 0.10 then 1 end) as bucket_0_10_pct,
    count(case when scored_base between 0.11 and 0.20 then 1 end) as bucket_11_20_pct,
    count(case when scored_base between 0.21 and 0.35 then 1 end) as bucket_21_35_pct,
    count(case when scored_base between 0.36 and 0.50 then 1 end) as bucket_36_50_pct,
    count(case when scored_base between 0.51 and 0.70 then 1 end) as bucket_51_70_pct,
    count(case when scored_base > 0.70 then 1 end) as bucket_above_70_pct
from {{ ref('stem_impact_analysis_stg') }}
group by
    class,
    donor,
    state,
    district,
    school_name,
    trainer_name
order by
    donor,
    state,
    district,
    school_name,
    trainer_name,
    class
