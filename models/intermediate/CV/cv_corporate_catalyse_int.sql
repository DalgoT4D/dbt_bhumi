with source as (
    select * from {{ ref('cv_corporate_catalyse_tracker_stg') }}
),

enriched as (
    select
        *,

        -- project classification (metrics 3 & 6): keyword match on event name
        case
            when lower(event_name) like '%young%scientist%' then 'Young Scientist'
            when lower(event_name) like '%stem%in%schools%' then 'STEM in Schools'
            when lower(event_name) like '%nakshatra%' then 'Nakshatra'
            when lower(event_name) like '%skill%ed%' then 'Skill-ed'
            when lower(event_name) like '%eco%champs%' then 'Eco Champs'
            else 'General'
        end as project_category,

        -- volunteer type classification (metric 4)
        -- Open, Unmatched, Matched → Adhoc | GRL Based, Projected → Regular | {} → Probono
        case
            when lower(btrim(corporate_event_type)) in ('open', 'unmatched', 'matched') then 'Adhoc'
            when lower(btrim(corporate_event_type)) in ('grl based', 'projected') then 'Regular'
            when btrim(corporate_event_type) = '{}' then 'Probono'
            else nullif(btrim(corporate_event_type), '')
        end as volunteer_type,

        -- partner type classification (metric 8): Projected, Matched, UnMatched, CSR Partner
        -- unmatched checked before matched to avoid false positive
        case
            when lower(corporate_event_type) like '%projected%' then 'Projected'
            when lower(corporate_event_type) like '%unmatched%' then 'UnMatched'
            when lower(corporate_event_type) like '%matched%' then 'Matched'
            when lower(corporate_event_type) like '%csr%' then 'CSR Partner'
            else nullif(btrim(corporate_event_type), '')
        end as partner_type,

        -- event closure / SLA indicator (metric 7): non-empty event_closed_by_all_aspect
        -- signals that event has been closed and donor impact report has been sent
        case
            when event_closed_by_all_aspect is not null
                and btrim(event_closed_by_all_aspect) not in ('', 'false', 'null')
            then true
            else false
        end as is_impact_report_sent

    from source
)

select * from enriched
