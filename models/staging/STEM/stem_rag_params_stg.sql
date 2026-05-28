{{ config(
  materialized='table',
  tags=["stem", "staging"]
) }}

with rag_params as (
    select
        -- identifiers
        Coalesce(Btrim(freq::text), '') as freq,
        Coalesce(Btrim(parameter::text), '') as parameter,
        Coalesce(Btrim(description::text), '') as description,

        -- thresholds
        case when Btrim(black_percent_comp::text) ~ '^[0-9]+(\.[0-9]+)?$' then (black_percent_comp::text)::numeric end as black_percent_comp,
        case when Btrim(red_percent_comp::text) ~ '^[0-9]+(\.[0-9]+)?$' then (red_percent_comp::text)::numeric end as red_percent_comp,
        case when Btrim(amber_percent_comp::text) ~ '^[0-9]+(\.[0-9]+)?$' then (amber_percent_comp::text)::numeric end as amber_percent_comp,
        case when Btrim(green_percent_comp::text) ~ '^[0-9]+(\.[0-9]+)?$' then (green_percent_comp::text)::numeric end as green_percent_comp,
        case when Btrim(gold_percent_comp::text) ~ '^[0-9]+(\.[0-9]+)?$' then (gold_percent_comp::text)::numeric end as gold_percent_comp
    from {{ source('Stem_gsheet_data', 'STEM_RAG_Params') }}
)

select distinct
    freq,
    parameter,
    description,
    black_percent_comp,
    red_percent_comp,
    amber_percent_comp,
    green_percent_comp,
    gold_percent_comp
from rag_params
where parameter != ''
