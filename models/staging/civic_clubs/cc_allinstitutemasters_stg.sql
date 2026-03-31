{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

select 
"ID" as id,
"City1"->> 'City' as city,
"State"->> 'State' as state,
"Institute" as institute,
"Institution_Kind" ->> 'Institution_Kind' as institution_kind


from {{ source('zc_bvms_data', 'All_Institute_Masters') }}