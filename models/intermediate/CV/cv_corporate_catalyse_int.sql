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
        (dr.invoice_date - source.event_end_date) as donor_reporting_sla_days,

        -- project classification (metrics 3 & 6): keyword match on event name
        case
            when lower(source.event_name) like '%young%scientist%' then 'Young Scientist'
            when lower(source.event_name) like '%stem%in%schools%' then 'STEM in Schools'
            when lower(source.event_name) like '%nakshatra%' then 'Nakshatra'
            when lower(source.event_name) like '%skill%ed%' then 'Skill-ed'
            when lower(source.event_name) like '%eco%champs%' then 'Eco Champs'
            else 'General'
        end as project_category,

        -- volunteer type classification (metric 4)
        -- Open, Unmatched, Matched → Adhoc | GRL Based, Projected → Regular | {} → Probono
        case
            when lower(btrim(source.corporate_event_type)) in ('open', 'unmatched', 'matched') then 'Adhoc'
            when lower(btrim(source.corporate_event_type)) in ('grl based', 'projected') then 'Regular'
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

        -- event closure / SLA indicator (metric 7)
        coalesce(
            source.event_closed_by_all_aspect is not null
            and btrim(source.event_closed_by_all_aspect) not in ('', 'false', 'null'), false
        ) as is_impact_report_sent,

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
        end as quarter

    from source
    left join donor_report as dr on source.event_id = dr.event_id
),

-- running event count per partner per FY, ordered by event date
-- each row sees only events up to and including itself in the FY
partner_stats as (
    select
        *,
        count(event_id) over (
            partition by corporate_partner_id, fy
            order by event_start_date
            rows between unbounded preceding and current row
        ) as partner_events_so_far_in_fy
    from enriched
)

select
    *,
    -- active:    this event contributes to the partner's activity in this FY (metric 15)
    (partner_events_so_far_in_fy >= 1) as is_active_partner,
    -- recurring: at the point of this event, the partner has already run >=1 event in this FY (metric 16)
    (partner_events_so_far_in_fy > 1) as is_recurring_partner,
    -- new:       this is the partner's first event in this FY (metric 17)
    (partner_events_so_far_in_fy = 1) as is_new_partner
from partner_stats
