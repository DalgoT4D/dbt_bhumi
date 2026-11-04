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
        nullif(btrim("Team"::text), '') as team,
        nullif(btrim("Reporting_Manager"::text), '') as reporting_manager,
        nullif(btrim("Date_of_Joining"::text), '') as date_of_joining,
        nullif(btrim("IGrow_Status"::text), '') as igrow_status
    from {{ source('hr_raw_data', 'Status_Tracker') }}
),

status_tracker as (
    select
        case when sl_no ~ '^[0-9]+$' then sl_no::integer end as sl_no,
        -- some IDs are typed in lowercase (c00326)
        upper(employee_id) as employee_id,
        -- names, teams and statuses are kept as entered so "Dr. Prahalathan KK", "MarComm" and
        -- dropdown values stay intact; a lone trailing " ." (e.g. "Haiderali .") is removed
        regexp_replace(name, ' [.]$', '') as name,
        team,
        reporting_manager,
        -- sheet stores dates as DD-Mon-YYYY (e.g. 14-Mar-2022), which validate_date reads unambiguously
        {{ validate_date('date_of_joining') }} as date_of_joining, -- noqa: LT02
        igrow_status
    from trimmed
)

select
    sl_no,
    employee_id,
    name,
    team,
    reporting_manager,
    date_of_joining,
    igrow_status
from status_tracker
