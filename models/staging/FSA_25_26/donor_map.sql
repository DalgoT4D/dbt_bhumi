{{ config(
  materialized='table',
  tags=["fsa_25_26", "staging"]
) }}

with donor_mapping_cleaned as (
    select
        -- NULLIF(BTRIM(id::TEXT), '')         as id,
        NULLIF(BTRIM(donor_id::TEXT), '')   as donor_id,
        NULLIF(BTRIM(fellow_id::TEXT), '')  as fellow_id,
        NULLIF(BTRIM(funding_year::TEXT), '')      as funding_year
    from {{ source('fellowship_school_app_25_26', 'donor_mapping') }}
)

select distinct * from donor_mapping_cleaned
where
    donor_id is not null
    and fellow_id is not null
