{{ config(
  materialized='table',
  tags=["finance", "prod"]
) }}

-- one row per expense voucher in the Zoho Lead Time Summary Report; the BI layer averages.
-- two SLA stages, per the dashboard spec:
--   stage 1 "Until RM approval"                      : added_time -> received_by_finance
--                                                      measured on ALL vouchers, any status
--   stage 2 "Until finance final approval & payment" : received_by_finance -> the LATER of the
--                                                      finance associate / finance manager sign-offs
--                                                      measured on VERIFIED vouchers ONLY,
--                                                      i.e. status 'Bhumi Verified' or the
--                                                      downstream 'Payment Made'
-- the two measures therefore sit on different row sets by design, not by accident. stage 2 is
-- null on every voucher that has not cleared verification -- rejected, for correction, still
-- pending -- even where the approval timestamps exist, so it never reports a finance
-- turnaround on an expense that was never actually verified.
-- durations are WORKING days (Mon-Fri) and are elapsed, not inclusive: a same-day turnaround
-- is 0 and Friday -> Monday is 1. Public holidays still count as working days because this
-- project holds no holiday calendar; macros/working_days_between.sql is the single place to
-- fix that once one exists.
-- no ROWS are dropped, so voucher counts still tie back to the finance team's own Zoho
-- report; the status rule narrows a measure, not the grain. use the is_awaiting_* flags to
-- keep in-flight vouchers out of the averages.
-- reads staging directly: single upstream, no joins, no re-grain.

with vouchers as (
    select
        id,
        no,
        zb_id,
        voucher_no,
        status,
        payment_status,
        pending_with,
        project,
        entity,
        event,
        donor_name,
        donor_project_name,
        donor_budget_line_item,
        vendor_name,
        type_field,
        bhumi_name_name,
        added_user,
        total_amount,
        amount_paid,
        bill_invoice_date,
        added_time,
        received_by_finance,
        manager_approved_time,
        finance_associate_approved_time,
        finance_manager_approved_time,
        finance_payment_time
    from {{ ref('lead_time_summary_report_stg') }}
),

stamped as (
    select
        *,
        added_time::date as raised_date,
        -- postgres greatest() ignores nulls, so this is the later sign-off where both exist
        -- and whichever one happened where only one does. the stage 2 clock stops at the
        -- FINAL finance approval, not the first finance touch.
        greatest(finance_associate_approved_time, finance_manager_approved_time) as finance_approved_time
    from vouchers
),

derived as (
    select
        *,
        finance_approved_time::date as finance_approved_date,
        -- gate for the stage 2 measure: the voucher has cleared verification. 'Payment Made'
        -- is the state downstream of 'Bhumi Verified', so both qualify; leaving it out would
        -- drop the vouchers that ran all the way through and bias the average upwards.
        -- matched on the exact source literals, which are confirmed clean at source.
        -- coalesced so a missing status reads as not eligible rather than unknown, keeping
        -- this a clean two-valued filter in the BI tool.
        coalesce(status in ('Bhumi Verified', 'Payment Made'), false) as is_finance_sla_eligible,
        date_trunc('month', added_time)::date as raised_month,
        -- indian financial year, april to march. the macro additionally reads an implausible
        -- date (outside 2015 .. a year ahead) as unknown rather than deriving a bogus year.
        {{ financial_year_start_year('added_time') }} as raised_financial_year_start_year
    from stamped
),

measured as (
    select
        *,
        {{ working_days_between('raised_date', 'received_by_finance') }} as stage_1_working_days,
        {{ working_days_between('received_by_finance', 'finance_approved_date') }} as stage_2_working_days
    from derived
)

select
    id as voucher_id,
    no as voucher_series,
    voucher_no,
    zb_id,
    status,
    is_finance_sla_eligible,
    payment_status,
    pending_with,
    project,
    entity,
    event,
    donor_name,
    donor_project_name,
    donor_budget_line_item,
    vendor_name,
    type_field,
    bhumi_name_name as raised_for,
    added_user as raised_by,
    total_amount,
    amount_paid,
    bill_invoice_date,
    -- stage boundaries, kept so any duration on this model can be recomputed or redefined
    added_time,
    raised_date,
    raised_month,
    raised_financial_year_start_year,
    {{ financial_year_label('raised_financial_year_start_year', 'fy_short') }} as raised_financial_year,
    received_by_finance,
    manager_approved_time,
    finance_associate_approved_time,
    finance_manager_approved_time,
    finance_approved_time,
    finance_payment_time,
    -- the two dashboard measures. negatives are suppressed rather than averaged: an approval
    -- stamped before the stage started is bad data, not a fast approval. has_invalid_timeline
    -- below is how you find and size those rows.
    -- stage 1 is open to every status; stage 2 and the end-to-end total are gated on
    -- is_finance_sla_eligible.
    case when stage_1_working_days >= 0 then stage_1_working_days end as working_days_until_rm_approval,
    case when is_finance_sla_eligible and stage_2_working_days >= 0 then stage_2_working_days end as working_days_until_finance_approval,
    case
        when is_finance_sla_eligible and stage_1_working_days >= 0 and stage_2_working_days >= 0 then stage_1_working_days + stage_2_working_days
    end as working_days_total_approval,
    case
        when finance_manager_approved_time is null and finance_associate_approved_time is null then null
        when finance_manager_approved_time is null then 'Finance Associate'
        when finance_associate_approved_time is null then 'Finance Manager'
        when finance_manager_approved_time >= finance_associate_approved_time then 'Finance Manager'
        else 'Finance Associate'
    end as final_finance_approver,
    received_by_finance is null as is_awaiting_finance_receipt,
    finance_approved_time is null as is_awaiting_finance_approval,
    coalesce(stage_1_working_days < 0, false) or coalesce(stage_2_working_days < 0, false) as has_invalid_timeline,
    -- reflects the dbt run, not the zoho extraction time
    current_date as data_as_of
from measured
order by
    raised_date desc nulls last,
    voucher_id asc
