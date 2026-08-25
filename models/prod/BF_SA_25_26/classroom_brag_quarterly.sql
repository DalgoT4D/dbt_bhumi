{{ config(
  materialized='table',
  tags=["fsa_25_26", "prod"]
) }}

select *
from {{ ref('class_stu_avg_quarterly') }}

union all

select *
from {{ ref('class_teaching_quarterly') }}
