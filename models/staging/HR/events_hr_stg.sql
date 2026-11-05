{{ config(
  materialized='table',
  tags=["hr", "staging"]
) }}

-- trims every value and turns empty cells into NULL;
-- number columns also drop thousand separators so "1,200" can be cast
with trimmed as (
    select
        nullif(btrim("FY"::text), '') as fy,
        nullif(btrim("Type"::text), '') as type,
        nullif(btrim("Date"::text), '') as event_date,
        nullif(btrim("Event_Name"::text), '') as event_name,
        nullif(btrim("Desc"::text), '') as description,
        nullif(btrim("Status"::text), '') as status,
        nullif(btrim(replace("No__of_Expected_Participants"::text, ',', '')), '') as no_of_expected_participants,
        nullif(btrim(replace("Number_of_Participants_"::text, ',', '')), '') as number_of_participants,
        nullif(btrim(replace("CSAT"::text, ',', '')), '') as csat
    from {{ source('hr_raw_data', 'Events') }}
),

-- Date has no year (e.g. "January 5"), so month and day are read here and the year comes from FY.
-- FY's first year is read from 2 or 4 digits so "2026-2027" and "FY 26-27" both give 2026
date_parts as (
    select
        *,
        right('20' || substring(fy from '[0-9]{4}|[0-9]{2}'), 4)::integer as fy_start_year,
        array_position(
            array['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec'],
            lower(left(substring(event_date from '[A-Za-z]+'), 3))
        ) as event_month_no,
        substring(event_date from '[0-9]{1,2}')::integer as event_day
    from trimmed
),

-- FY runs April–March: April–December take the FY's first year, January–March the second
-- (FY 2026-2027: May 10 → 2026-05-10, January 5 → 2027-01-05)
month_start as (
    select
        *,
        make_date(fy_start_year + (event_month_no <= 3)::integer, event_month_no, 1) as event_month_start
    from date_parts
),

events as (
    select
        -- YYYY-YYYY like every HR model (FY 26-27 → 2026-2027)
        fy_start_year::text || '-' || (fy_start_year + 1)::text as fy,
        initcap(type) as type,
        -- day is checked against the month's length so a typo like "February 30" becomes NULL instead of failing
        case
            when event_day between 1 and extract(day from event_month_start + interval '1 month - 1 day')
                then event_month_start + (event_day - 1)
        end as event_date,
        event_name,
        description,
        initcap(status) as status,
        case when no_of_expected_participants ~ '^[0-9]+$' then no_of_expected_participants::integer end as no_of_expected_participants,
        case when number_of_participants ~ '^[0-9]+$' then number_of_participants::integer end as number_of_participants,
        case when csat ~ '^[0-9]+([.][0-9]+)?$' then csat::numeric end as csat
    from month_start
)

select
    fy,
    type,
    event_date,
    event_name,
    description,
    status,
    no_of_expected_participants,
    number_of_participants,
    csat
from events
where event_name is not null
