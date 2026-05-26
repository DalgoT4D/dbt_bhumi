{{ config(
    materialized='table',
    tags=["civic_clubs"]
) }}

select
    batch_name,
    specialization,
    beneficiary_status,
    count(beneficiary_id) as beneficiary_count
from {{ ref('cc_beneficiary_skilled_stg') }}
group by batch_name, specialization, beneficiary_status
