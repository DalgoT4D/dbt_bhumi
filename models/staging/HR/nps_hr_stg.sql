{{ config(
  materialized='table',
  tags=["hr", "staging"]
) }}

-- trims every value and turns empty cells into NULL;
-- number columns also drop thousand separators and % signs so "1,200" and "84%" can be cast
with trimmed as (
    select
        nullif(btrim("Sl_No"::text), '') as sl_no,
        nullif(btrim("FY"::text), '') as fy,
        nullif(btrim("Survey_Month"::text), '') as survey_month,
        nullif(btrim(regexp_replace("CD_Head_Count"::text, '[,%]', '', 'g')), '') as cd_head_count,
        nullif(btrim(regexp_replace("Participants"::text, '[,%]', '', 'g')), '') as participants,
        nullif(btrim(regexp_replace("Named_Responses"::text, '[,%]', '', 'g')), '') as named_responses,
        nullif(btrim(regexp_replace("Participation_Percent"::text, '[,%]', '', 'g')), '') as participation_percent,
        nullif(btrim(regexp_replace("NPS"::text, '[,%]', '', 'g')), '') as nps,
        nullif(btrim(regexp_replace("Weighted_Average"::text, '[,%]', '', 'g')), '') as weighted_average
    from {{ source('hr_raw_data', 'Bhumi_Internal_CD_Survey_NPS') }}
),

-- Survey_Month mixes "Dec-22" and "August 2026"; reading the month name and year separately
-- handles both (and variants like "Sept 2026" or "Mar 24")
survey_month_parts as (
    select
        *,
        lower(left(substring(survey_month from '^[A-Za-z]+'), 3)) as survey_month_abbr,
        substring(survey_month from '[0-9]+$') as survey_month_year
    from trimmed
),

nps_survey as (
    select
        case when sl_no ~ '^[0-9]+$' then sl_no::integer end as sl_no,
        upper(fy) as fy,
        -- Mon-YYYY (e.g. Dec-22 → Dec-2022, August 2026 → Aug-2026)
        case
            when
                survey_month_abbr in ('jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec')
                and survey_month_year ~ '^([0-9]{2}|[0-9]{4})$'
                then initcap(survey_month_abbr) || '-' || right('20' || survey_month_year, 4)
        end as survey_month,
        case when cd_head_count ~ '^[0-9]+$' then cd_head_count::integer end as cd_head_count,
        case when participants ~ '^[0-9]+$' then participants::integer end as participants,
        case when named_responses ~ '^[0-9]+$' then named_responses::integer end as named_responses,
        case when participation_percent ~ '^-?[0-9]+([.][0-9]+)?$' then participation_percent::numeric end as participation_percent,
        -- NPS ranges from -100 to 100, so negatives are kept
        case when nps ~ '^-?[0-9]+([.][0-9]+)?$' then nps::numeric end as nps,
        case when weighted_average ~ '^-?[0-9]+([.][0-9]+)?$' then weighted_average::numeric end as weighted_average
    from survey_month_parts
    -- drops completely blank sheet rows
    where coalesce(
        sl_no, fy, survey_month, cd_head_count, participants,
        named_responses, participation_percent, nps, weighted_average
    ) is not null
)

select
    sl_no,
    fy,
    survey_month,
    cd_head_count,
    participants,
    named_responses,
    participation_percent,
    nps,
    weighted_average
from nps_survey
