{{ config(
  materialized='table',
  tags=["finance", "prod"]
) }}

-- one row per Zoho budget line item; the BI layer aggregates.
-- reads staging directly: single upstream, no joins, no re-grain.
-- when voucher actuals are added (project_utilisation_summary_report_stg), put the
-- conforming logic in models/intermediate/Finance/ rather than joining here.

with budget_line_items as (
    select
        id,
        series,
        status,
        project,
        location,
        donor_name,
        donor_project_name,
        donor_budget_line_item,
        entity_name,
        donation_type,
        donation_source,
        financial_year,
        mou_start_date,
        mou_end_date,
        budget_master_id,
        budget_amount,
        total_paid,
        utilization_percent
    from {{ ref('budget_line_item_master_report_stg') }}
),

cleaned as (
    select
        id as budget_line_item_id,
        series as budget_line_item_code,
        budget_master_id,
        status,
        financial_year,
        -- sort key for the period filter
        case
            when financial_year ~ '\d{4}' then (substring(financial_year, '(\d{4})'))::integer
            when financial_year ~ '\d{2}\s*[-/]\s*\d{2}' then 2000 + (substring(financial_year, '(\d{2})'))::integer
        end as financial_year_start_year,
        -- whitespace-normalised display values; no initcap (would mangle STEM, CSR, HDFC)
        nullif(regexp_replace(btrim(project), '\s+', ' ', 'g'), '') as program,
        nullif(regexp_replace(btrim(donor_name), '\s+', ' ', 'g'), '') as donor_name,
        nullif(regexp_replace(btrim(donor_project_name), '\s+', ' ', 'g'), '') as donor_project_name,
        nullif(regexp_replace(btrim(donor_budget_line_item), '\s+', ' ', 'g'), '') as donor_budget_line_item,
        nullif(regexp_replace(btrim(entity_name), '\s+', ' ', 'g'), '') as entity_name,
        donation_type,
        donation_source,
        location,
        mou_start_date,
        mou_end_date,
        -- amounts kept as-is: never coalesced to 0, never filtered out
        budget_amount,
        total_paid,
        -- zoho's own percentage is COMMITMENT basis: verified to equal
        -- (budget_amount - balance_with_payable) / budget_amount on 3303 of 3387 rows,
        -- so it counts approved-but-unpaid vouchers and normally exceeds the cash-basis
        -- utilisation_pct below. the two are different measures, not a discrepancy.
        utilization_percent as utilisation_pct_reported_incl_payables
    from budget_line_items
),

measured as (
    select
        *,
        budget_amount - total_paid as unspent_amount,
        round(total_paid::numeric / nullif(budget_amount, 0) * 100, 2) as utilisation_pct
    from cleaned
)

select
    budget_line_item_id,
    budget_line_item_code,
    budget_master_id,
    financial_year,
    financial_year_start_year,
    program,
    donor_name,
    donor_project_name,
    donor_budget_line_item,
    entity_name,
    donation_type,
    donation_source,
    location,
    status,
    mou_start_date,
    mou_end_date,
    budget_amount,
    total_paid,
    unspent_amount,
    utilisation_pct,
    utilisation_pct_reported_incl_payables,
    -- flat >100%; no tolerance allowance, since tolerance_balance is out of scope.
    -- null (not false) when unknown, so "unknown" never reads as "fine".
    case
        when budget_amount is null or total_paid is null then null::boolean
        when budget_amount <= 0 then null::boolean
        else total_paid > budget_amount
    end as is_overspent,
    budget_amount is null or total_paid is null as is_amount_missing,
    -- reflects the dbt run, not the Zoho extraction time
    current_date as data_as_of
from measured
order by
    financial_year_start_year,
    program,
    donor_name,
    budget_line_item_id
