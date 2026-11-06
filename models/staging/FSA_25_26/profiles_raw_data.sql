{{ config(
  materialized='table',
  tags=["fsa_25_26", "staging"]
) }}

with profiles as (
    select
        NULLIF(BTRIM(id::TEXT),'') as user_id,
        NULLIF(INITCAP(BTRIM(REGEXP_REPLACE(CONCAT_WS(' ', first_name, last_name), '\s+', ' ', 'g'))),'') as full_name,
        is_active
    from {{ source('fellowship_school_app_25_26', 'profiles_25_26') }}
)

select * from profiles
where
    user_id is not NULL
    -- and full_name is not NULL
