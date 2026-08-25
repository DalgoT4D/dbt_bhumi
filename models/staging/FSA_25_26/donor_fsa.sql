{{ config(
  materialized='table',
  tags=["fsa_25_26", "staging"]
) }}


with donors_cleaned as (
    select
        NULLIF(BTRIM(id::TEXT), '') as donor_id,
        NULLIF(INITCAP(BTRIM(name::TEXT)), '') as donor_name
    from {{ source('fellowship_school_app_25_26', 'donors') }}
)

select distinct * from donors_cleaned
where
    donor_id is not null
    and donor_name is not null
