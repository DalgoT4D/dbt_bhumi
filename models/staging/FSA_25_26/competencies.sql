{{ config(
  materialized='table',
  tags=["fsa_25_26", "staging"]
) }}

with competencies as (
    select 
        NULLIF(BTRIM(id::TEXT),'') as id_competency,
        NULLIF(BTRIM(name::TEXT),'') as name_competency,
        NULLIF(BTRIM(is_active::TEXT),'')::BOOLEAN as is_active
    from {{ source('fellowship_school_app_25_26', 'competencies_25_26') }}
)

select * from competencies
where
    id_competency is not NULL
