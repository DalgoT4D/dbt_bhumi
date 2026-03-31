with master_sheet_impact as (
    select
        -- identifiers
        case when Btrim(class::text) ~ '^\d+$' then (class::text)::integer end as class,
        Coalesce(Initcap(Btrim(donor::text)), '') as donor,
        Coalesce(Initcap(Btrim(state::text)), '') as state,
        Coalesce(Btrim(bvms_id::text), '') as bvms_id,
        Coalesce(Initcap(Btrim(district::text)), '') as district,
        Coalesce(Btrim(baseline_id::text), '') as baseline_id,
        Coalesce(Initcap(Btrim(school_name::text)), '') as school_name,
        Coalesce(Initcap(Btrim(student_name::text)), '') as student_name,
        Coalesce(Initcap(Btrim(trainer_name::text)), '') as trainer_name,

        -- baseline scores
        case when Btrim(_tech_base::text) ~ '^[0-9]+(\.[0-9]+)?$' then (_tech_base::text)::numeric end as tech_base,
        case when Btrim(_maths_base::text) ~ '^[0-9]+(\.[0-9]+)?$' then (_maths_base::text)::numeric end as maths_base,
        case when Btrim(_science_base::text) ~ '^[0-9]+(\.[0-9]+)?$' then (_science_base::text)::numeric end as science_base,
        case when Btrim(_scored_base::text) ~ '^[0-9]+(\.[0-9]+)?$' then (_scored_base::text)::numeric end as scored_base,
        case when Btrim(knowledge_base::text) ~ '^[0-9]+(\.[0-9]+)?$' then (knowledge_base::text)::numeric end as knowledge_base,
        case when Btrim(application_base::text) ~ '^[0-9]+(\.[0-9]+)?$' then (application_base::text)::numeric end as application_base,
        case when Btrim(total_marks_base::text) ~ '^[0-9]+(\.[0-9]+)?$' then (total_marks_base::text)::numeric end as total_marks_base,
        case when Btrim(understanding_base::text) ~ '^[0-9]+(\.[0-9]+)?$' then (understanding_base::text)::numeric end as understanding_base,

        -- endline scores
        case when Btrim(_tech_end::text) ~ '^[0-9]+(\.[0-9]+)?$' then (_tech_end::text)::numeric end as tech_end,
        case when Btrim(_maths_end::text) ~ '^[0-9]+(\.[0-9]+)?$' then (_maths_end::text)::numeric end as maths_end,
        case when Btrim(_science_end::text) ~ '^[0-9]+(\.[0-9]+)?$' then (_science_end::text)::numeric end as science_end,
        case when Btrim(_scored_end::text) ~ '^[0-9]+(\.[0-9]+)?$' then (_scored_end::text)::numeric end as scored_end,
        case when Btrim(knowledge_end::text) ~ '^[0-9]+(\.[0-9]+)?$' then (knowledge_end::text)::numeric end as knowledge_end,
        case when Btrim(application_end::text) ~ '^[0-9]+(\.[0-9]+)?$' then (application_end::text)::numeric end as application_end,
        case when Btrim(total_marks_end::text) ~ '^[0-9]+(\.[0-9]+)?$' then (total_marks_end::text)::numeric end as total_marks_end,
        case when Btrim(understanding_end::text) ~ '^[0-9]+(\.[0-9]+)?$' then (understanding_end::text)::numeric end as understanding_end
    from {{ source('Stem_gsheet_data', 'Master_Sheet_Impact') }}
)

select distinct
    class,
    donor,
    state,
    bvms_id,
    district,
    baseline_id,
    school_name,
    student_name,
    trainer_name,
    tech_base,
    maths_base,
    science_base,
    scored_base,
    knowledge_base,
    application_base,
    total_marks_base,
    understanding_base,
    tech_end,
    maths_end,
    science_end,
    scored_end,
    knowledge_end,
    application_end,
    total_marks_end,
    understanding_end
from master_sheet_impact
where school_name != ''
