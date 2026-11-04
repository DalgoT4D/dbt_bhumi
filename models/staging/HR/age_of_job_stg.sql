{{ config(
  materialized='table',
  tags=["hr", "staging"]
) }}

-- collapses line breaks, tabs, non-breaking and zero-width spaces into a single space,
-- trims the value and turns empty cells into NULL
with trimmed as (
    select
        nullif(btrim("S_no"::text), '') as s_no,
        nullif(btrim("Quarter"::text), '') as quarter,
        nullif(btrim("Position"::text), '') as position,
        nullif(btrim("Category"::text), '') as category,
        nullif(btrim("Status"::text), '') as status,
        nullif(btrim("Owner"::text), '') as owner,
        nullif(btrim("Hiring_Manager"::text), '') as hiring_manager,
        nullif(btrim("JD_Posted_Date"::text), '') as jd_posted_date,
        nullif(btrim("Republished_date_"::text), '') as republished_date,
        nullif(btrim("Age_of_Job"::text), '') as age_of_job
    from {{ source('hr_raw_data', 'Age_of_Jobs') }}
),

-- the HR sheet stores dates as M/D/YYYY (e.g. 3/13/2026). validate_date tries DD/MM first, so 4/5/2026
-- would become 4 May; rewriting to YYYY-M-D first makes the macro read them as month/day
iso_dates as (
    select
        *,
        regexp_replace(jd_posted_date, '^(\d{1,2})/(\d{1,2})/(\d{4})$', '\3-\1-\2') as jd_posted_date_iso,
        regexp_replace(republished_date, '^(\d{1,2})/(\d{1,2})/(\d{4})$', '\3-\1-\2') as republished_date_iso
    from trimmed
),

age_of_jobs as (
    select
        case when s_no ~ '^[0-9]+$' then s_no::integer end as s_no,
        upper(replace(quarter, ' ', '')) as quarter,
        position,
        initcap(category) as category,
        initcap(status) as status,
        initcap(owner) as owner,
        initcap(hiring_manager) as hiring_manager,
        {{ validate_date('jd_posted_date_iso') }} as jd_posted_date, -- noqa: LT02
        {{ validate_date('republished_date_iso') }} as republished_date, -- noqa: LT02
        case when age_of_job ~ '^-?[0-9]+$' then age_of_job::integer end as age_of_job
    from iso_dates
)

select
    s_no,
    quarter,
    position,
    category,
    status,
    owner,
    hiring_manager,
    jd_posted_date,
    republished_date,
    age_of_job
from age_of_jobs
-- drops blank / formatting-only sheet rows that carry no job
where position is not null
