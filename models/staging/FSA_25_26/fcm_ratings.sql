{{ config(
  materialized='table',
  tags=["fsa_25_26", "staging"]
) }}

with fcm_ratings as (
    select 
        NULLIF(BTRIM(id::TEXT),'') as fcm_rating_id,
        NULLIF(BTRIM(fcm_update_id::TEXT),'') as fcm_update_id,
        NULLIF(BTRIM(competency_mapping_id::TEXT),'') as competency_mapping_id,
        NULLIF(BTRIM(rating::TEXT),'')::INTEGER as rating

    from {{ source('fellowship_school_app_25_26', 'fcm_ratings_25_26') }}
)

select * from fcm_ratings
where
    fcm_rating_id is not NULL
    and fcm_update_id is not NULL
    and competency_mapping_id is not NULL
