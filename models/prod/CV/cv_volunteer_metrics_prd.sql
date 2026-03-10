-- Event-level fact table for Corporate Volunteering dashboard.
-- Supports all 17 metrics via BI-tool aggregation:
--   1.  Total Volunteers           → sum(actual_no_of_volunteers)
--   2.  Total Volunteering Hours   → sum(total_volunteering_hours)
--   3.  Volunteers by Project      → group by project_category, sum(actual_no_of_volunteers)
--   4.  Volunteers by Type         → group by volunteer_type, sum(actual_no_of_volunteers)
--   5.  Volunteers by Corporate    → group by corporate_partner_name, sum(actual_no_of_volunteers)
--   6.  Events by Project          → group by project_category, count(distinct event_id)
--   7.  Event Closure SLA          → filter/group by is_impact_report_sent, event_end_date
--   8.  Partners by Type           → group by partner_type, count(distinct corporate_partner_id)
--   9.  Total Budget               → sum(event_planned_budget)
--   10. Total Expenses             → sum(expense_incurred)
--   11. Beneficiaries by Date      → group by event_start_date, sum(total_no_of_beneficiary)
--   12. Beneficiaries by Project   → group by project_category, sum(total_no_of_beneficiary)
--   13. FY                         → filter/group by fy
--   14. Quarter                    → filter/group by quarter
--   15. Active Partners            → filter is_active_partner, count(distinct corporate_partner_id)
--   16. Recurring Partners         → filter is_recurring_partner, count(distinct corporate_partner_id)
--   17. New Partners               → filter is_new_partner, count(distinct corporate_partner_id)

with source as (
    select * from {{ ref('cv_corporate_catalyse_int') }}
)

select
    -- identifiers
    id,
    event_id,

    -- event attributes
    event_name,
    project_category,
    event_type,
    corporate_event_type,
    volunteer_type,
    partner_type,
    corporate_partner_id,
    corporate_partner_name,
    coordinator_name,
    event_city,
    event_start_date,
    event_end_date,
    type_of_implementation,

    -- time dimensions (metrics 13–14)
    fy,
    quarter,

    -- closure / SLA (metric 7)
    is_impact_report_sent,
    event_closed_by_all_aspect,
    donor_reporting_sla_days,

    -- volunteer metrics (metrics 1–5)
    actual_no_of_volunteers,
    expected_no_of_volunteers,
    total_volunteering_hours,

    -- budget & expense (metrics 9–10)
    event_planned_budget,
    total_spent as expense_incurred,

    -- beneficiary metrics (metrics 11–12)
    total_no_of_beneficiary,

    -- partner flags (metrics 15–17)
    is_active_partner,
    is_recurring_partner,
    is_new_partner

from source
order by
    fy,
    quarter,
    event_start_date,
    corporate_partner_name,
    event_name
