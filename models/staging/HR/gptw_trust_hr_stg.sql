{{ config(
  materialized='table',
  tags=["hr", "staging"]
) }}

-- trims every value and turns empty cells into NULL;
-- Trust_Index also drops thousand separators and % signs so "82%" can be cast
with trimmed as (
    select
        nullif(btrim("Sl_No"::text), '') as sl_no,
        nullif(btrim("FY_"::text), '') as fy,
        nullif(btrim("Survey_Month"::text), '') as survey_month,
        nullif(btrim(regexp_replace("Trust_Index"::text, '[,%]', '', 'g')), '') as trust_index
    from {{ source('hr_raw_data', 'GPTW_Trust_Index_') }}
),

-- month name and year are read separately so "June 2021", "Jun-21" and "Sept 2026" all work;
-- FY's first year is read the same way so "FY 2021-22" and "2021-2022" both give 2021
survey_month_parts as (
    select
        *,
        right('20' || substring(fy from '[0-9]{4}|[0-9]{2}'), 4)::integer as fy_start_year,
        lower(left(substring(survey_month from '^[A-Za-z]+'), 3)) as survey_month_abbr,
        substring(survey_month from '[0-9]+$') as survey_month_year
    from trimmed
),

gptw_trust as (
    select
        case when sl_no ~ '^[0-9]+$' then sl_no::integer end as sl_no,
        -- YYYY-YYYY like every HR model (FY 2021-22 → 2021-2022)
        fy_start_year::text || '-' || (fy_start_year + 1)::text as fy,
        -- Mon-YYYY like nps_hr_stg (June 2021 → Jun-2021)
        case
            when
                survey_month_abbr in ('jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec')
                and survey_month_year ~ '^([0-9]{2}|[0-9]{4})$'
                then initcap(survey_month_abbr) || '-' || right('20' || survey_month_year, 4)
        end as survey_month,
        case when trust_index ~ '^[0-9]+([.][0-9]+)?$' then trust_index::numeric end as trust_index
    from survey_month_parts
    -- drops completely blank sheet rows
    where coalesce(sl_no, fy, survey_month, trust_index) is not null
)

select
    sl_no,
    fy,
    survey_month,
    trust_index
from gptw_trust
