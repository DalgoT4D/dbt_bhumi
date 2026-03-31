with source as (
    select * from {{ source('Stem_gsheet_data', 'Consolidated_TCM') }}
),

tcm as (
    select
        -- identifiers
        coalesce(btrim(sl_no::text), '') as sl_no,
        coalesce(initcap(btrim(name_of_the_trainer::text)), '') as trainer_name,
        coalesce(initcap(btrim(trainer_location::text)), '') as trainer_location,
        coalesce(initcap(btrim(donor::text)), '') as donor,
        coalesce(initcap(btrim(manager::text)), '') as manager,
        coalesce(initcap(btrim(status::text)), '') as status,

        -- assessment scores
        case when btrim(q_2::text) ~ '^[0-9.]+$' then (q_2::text)::numeric end as q_2,
        case when btrim(q_3::text) ~ '^[0-9.]+$' then (q_3::text)::numeric end as q_3,
        case when btrim(q_4::text) ~ '^[0-9.]+$' then (q_4::text)::numeric end as q_4,
        case when btrim(baseline::text) ~ '^[0-9.]+$' then (baseline::text)::numeric end as baseline,
        case when btrim(improvement_::text) ~ '^[0-9.]+$' then (improvement_::text)::numeric end as improvement,
        case when btrim(_2025_26_consolidated_rating_::text) ~ '^[0-9.]+$' then (_2025_26_consolidated_rating_::text)::numeric end as consolidated_rating_2025_26

    from source
)

select distinct
    sl_no,
    trainer_name,
    trainer_location,
    donor,
    manager,
    status,
    q_2,
    q_3,
    q_4,
    baseline,
    improvement,
    consolidated_rating_2025_26
from tcm
where trainer_name != ''