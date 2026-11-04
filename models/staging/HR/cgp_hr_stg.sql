{{ config(
  materialized='table',
  tags=["hr", "staging"]
) }}

-- trims every value and turns empty cells into NULL
with trimmed as (
    select
        nullif(btrim("Sl_No"::text), '') as sl_no,
        nullif(btrim("Employee_ID"::text), '') as employee_id,
        nullif(btrim("Name"::text), '') as name,
        nullif(btrim("Effective_Date_"::text), '') as effective_date,
        nullif(btrim("Current_Designation"::text), '') as current_designation,
        nullif(btrim("New_Designation"::text), '') as new_designation,
        nullif(btrim("Type_"::text), '') as type
    from {{ source('hr_raw_data', 'CGP') }}
),

cgp as (
    select
        case when sl_no ~ '^[0-9]+$' then sl_no::integer end as sl_no,
        upper(employee_id) as employee_id,
        initcap(name) as name,
        -- sheet stores dates as D-Mon-YYYY (e.g. 1-Jun-2024), which validate_date reads unambiguously
        {{ validate_date('effective_date') }} as effective_date, -- noqa: LT02
        current_designation,
        new_designation,
        -- N/A / NA / - in the sheet mean no value
        case when upper(type) in ('N/A', 'NA', '-') then null else type end as type
    from trimmed
    -- drops completely blank sheet rows
    -- where coalesce(
    --     sl_no, employee_id, name, effective_date,
    --     current_designation, new_designation, type
    -- ) is not null
)

select
    sl_no,
    employee_id,
    name,
    effective_date,
    current_designation,
    new_designation,
    type
from cgp
