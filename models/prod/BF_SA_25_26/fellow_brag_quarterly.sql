{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

select *
from {{ ref('fellow_checkins_quarterly') }}

union all

select *
from {{ ref('fellow_fcm_quarterly') }}

union all

select *
from {{ ref('fellow_odc_quarterly') }}
