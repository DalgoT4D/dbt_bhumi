{{ config(
  materialized='table',
  tags=["cv", "int"]
) }}

with source as (
    select * from {{ ref('cv_corporate_catalyse_tracker_stg') }}
),

donor_report as (
    select * from {{ ref('cv_donor_report_stg') }}
),

enriched as (
    select
        source.*,
        -- donor reporting SLA: days between event end and donor invoice date (metric 7)
        coalesce(
            source.event_closed_by_all_aspect is not null
            and btrim(source.event_closed_by_all_aspect) not in ('', 'false', 'null'), false
        ) as is_impact_report_sent,
        dr.tat_days as donor_reporting_sla_days,
        ((dr.tat_days) <= 6) as is_donor_sla_met,
        -- dr.tat_days,

        -- project classification (metrics 3 & 6): keyword match on event name
        case
            when lower(source.event_name) like '%young%scientist%' then 'Young Scientist'
            when lower(source.event_name) like '%stem%in%schools%' then 'STEM in Schools'
            when lower(source.event_name) like '%nakshatra%' then 'Nakshatra'
            when lower(source.event_name) like '%skill%ed%' then 'Skill-ed'
            when lower(source.event_name) like '%eco%champs%' then 'Eco Champs'
            else 'CV'
        end as project_category,

        -- volunteer type classification (metric 4)
        -- Open, Unmatched, Matched → Adhoc | GRL Based, Projected → Regular | {} → Probono
        case
            when lower(btrim(source.corporate_event_type)) in ('open', 'unmatched', 'matched') then 'Others'
            when lower(btrim(source.corporate_event_type)) in ('grl based', 'projected') then 'Projected'
            when btrim(source.corporate_event_type) = '{}' then ''
            else nullif(btrim(source.corporate_event_type), '')
        end as volunteer_type,

        -- partner type classification (metric 8): Projected, Matched, UnMatched, CSR Partner
        -- unmatched checked before matched to avoid false positive
        case
            when lower(source.corporate_event_type) like '%projected%' then 'Projected'
            when lower(source.corporate_event_type) like '%unmatched%' then 'UnMatched'
            when lower(source.corporate_event_type) like '%matched%' then 'Matched'
            when lower(source.corporate_event_type) like '%csr%' then 'CSR Partner'
            else nullif(btrim(source.corporate_event_type), '')
        end as partner_type,

        -- FY: April–March cycle (e.g. Apr 2025 → Mar 2026 = "2025-2026") (metric 13)
        case
            when extract(month from source.event_start_date) >= 4
                then
                    extract(year from source.event_start_date)::integer::text
                    || '-'
                    || (extract(year from source.event_start_date) + 1)::integer::text
            else
                (extract(year from source.event_start_date) - 1)::integer::text
                || '-'
                || extract(year from source.event_start_date)::integer::text
        end as fy,

        -- quarter within FY: Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar (metric 14)
        case
            when extract(month from source.event_start_date) in (4, 5, 6) then 'Q1'
            when extract(month from source.event_start_date) in (7, 8, 9) then 'Q2'
            when extract(month from source.event_start_date) in (10, 11, 12) then 'Q3'
            when extract(month from source.event_start_date) in (1, 2, 3) then 'Q4'
        end as quarter,
        to_char(source.event_start_date, 'Month') as month,

        -- numeric FY / quarter for comparing periods: shifting the date back 3 months lines Apr–Mar up with Jan–Dec
        -- (e.g. Apr 2025 → fy_start_year 2025, quarter_no 1 | Mar 2026 → fy_start_year 2025, quarter_no 4)
        extract(year from source.event_start_date - interval '3 months')::integer as fy_start_year,
        extract(quarter from source.event_start_date - interval '3 months')::integer as quarter_no,

        -- partner identity for partner metrics (15–19): partner name, case and space insensitive
        case
            when btrim(source.corporate_partner_name) in ('', '{}') then null
            else lower(btrim(source.corporate_partner_name))
        end as partner_key

    from source
    left join donor_report as dr on source.event_id = dr.event_id
),

-- running event count per partner per FY, ordered by event date
-- each row sees only events up to and including itself in the FY
partner_stats as (
    select
        *,
        count(event_id) over (
            partition by corporate_partner_id, fy, quarter, month
            order by event_start_date
            rows between unbounded preceding and current row
        ) as partner_events_so_far_in_fy
    from enriched
),

-- one row per partner per FY they had an activity in, with their first active quarter of that FY
partner_fy as (
    select
        partner_key,
        fy_start_year,
        min(quarter_no) as first_quarter_no_in_fy
    from enriched
    where partner_key is not null and fy_start_year is not null
    group by partner_key, fy_start_year
),

-- most recent earlier FY the partner had an activity in (null = no activity on BVMS before this FY)
partner_fy_history as (
    select
        *,
        lag(fy_start_year) over (partition by partner_key order by fy_start_year) as last_active_fy_start_year
    from partner_fy
)

-- partner flags are evaluated per partner per FY-quarter, so every event of a partner in a quarter carries the same flags.
-- new / retained / dormant reactivated depend on history before this FY and are mutually exclusive;
-- annual depends on earlier quarters of this FY and can overlap with them.
select
    ps.*,
    -- active:              partner has at least one activity in this FY-quarter (metric 15)
    (pfh.partner_key is not null) as is_active_partner,
    -- new:                 no activity record before this FY at all (metric 16)
    coalesce(pfh.partner_key is not null and pfh.last_active_fy_start_year is null, false) as is_new_partner,
    -- dormant reactivated: last activity before this FY is older than the previous FY (metric 17)
    coalesce(pfh.last_active_fy_start_year < pfh.fy_start_year - 1, false) as is_dormant_reactivated_partner,
    -- retained:            last activity before this FY is from the previous FY (metric 18)
    coalesce(pfh.last_active_fy_start_year = pfh.fy_start_year - 1, false) as is_retained_partner,
    -- annual:              partner also had an activity in an earlier quarter of this FY (metric 19)
    coalesce(ps.quarter_no > pfh.first_quarter_no_in_fy, false) as is_annual_partner
from partner_stats as ps
left join partner_fy_history as pfh
    on
        ps.partner_key = pfh.partner_key
        and ps.fy_start_year = pfh.fy_start_year
