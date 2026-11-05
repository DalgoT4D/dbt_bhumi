{{ config(
  materialized='table',
  tags=["cv", "prod"]
) }}

-- Partner metrics per FY-quarter (partner = partner name, activity = event start date on BVMS).
-- new, dormant reactivated and retained never overlap; retained and annual never overlap;
-- new and dormant reactivated can overlap with annual in a partner's later quarters of the FY.
-- Counts are distinct partners per quarter, so they cannot be summed across quarters for an FY total
-- (use cv_volunteer_metrics_prd with count(distinct partner_key) for that).

with source as (
    select * from {{ ref('cv_corporate_catalyse_int') }}
)

select
    fy,
    quarter,
    count(distinct partner_key) as active_partner_count,
    count(distinct case when is_new_partner then partner_key end) as new_partner_count,
    count(distinct case when is_dormant_reactivated_partner then partner_key end) as dormant_reactivated_partner_count,
    count(distinct case when is_retained_partner then partner_key end) as retained_partner_count,
    count(distinct case when is_annual_partner then partner_key end) as annual_partner_count
from source
where is_active_partner
group by fy, quarter
order by fy, quarter
