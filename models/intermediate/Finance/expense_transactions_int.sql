{{ config(
  materialized='table',
  tags=["finance", "int"]
) }}

-- Budget Line Item Master joined to the two Zoho bill forms, so budget details and bill
-- details sit side by side on one row.
--
-- the join is on six fields: financial year, entity name, donor name, project, donor
-- project name and donor budget line item. financial year is derived from the bill date,
-- since only the master stores it. expense category is NOT a join key - the master is
-- budget-grain and has no expense head at all.
--
-- full outer join, so nothing from either side is lost: a budget line with no spend keeps
-- its budget_* columns with null bill columns, and a bill with no budget line keeps its
-- bill columns with null budget_* columns.

with budget_lines as (
    select
        id as budget_line_item_id,
        series as budget_line_item_code,
        budget_master_id,
        status as budget_line_status,
        budget_amount,
        total_paid as budget_total_paid,
        donation_type,
        donation_source,
        location as budget_location,
        mou_start_date,
        mou_end_date,
        -- whitespace-normalised join keys; no initcap (would mangle STEM, CSR, HDFC).
        -- normalisation is applied identically on both sides of the join.
        nullif(regexp_replace(btrim(financial_year), '\s+', ' ', 'g'), '') as key_financial_year,
        nullif(regexp_replace(btrim(entity_name), '\s+', ' ', 'g'), '') as key_entity_name,
        nullif(regexp_replace(btrim(donor_name), '\s+', ' ', 'g'), '') as key_donor_name,
        nullif(regexp_replace(btrim(project), '\s+', ' ', 'g'), '') as key_project,
        nullif(regexp_replace(btrim(donor_project_name), '\s+', ' ', 'g'), '') as key_donor_project_name,
        nullif(regexp_replace(btrim(donor_budget_line_item), '\s+', ' ', 'g'), '') as key_donor_budget_line_item
    from {{ ref('budget_line_item_master_report_stg') }}
),

approved_bills as (
    select
        'all_approved_bills' as source_table,
        id as transaction_id,
        new_parent_id_voucher_no as transaction_no,
        new_parent_id_bill_invoice_date::date as transaction_date,
        amount as transaction_amount,
        gross_amount_tax_applied as gross_amount,
        tax_amount,
        expense_category,
        expense_location,
        new_parent_id_vendor_name as vendor_name,
        new_parent_id_bhumi_name as bhumi_name,
        event,
        new_parent_id_status as source_status,
        new_parent_id_payment_status as payment_status,
        description1 as description,
        new_parent_id_entity as entity_name,
        donor_name,
        project,
        donor_project_name,
        donor_budget_line_item
    from {{ ref('all_approved_bills_stg') }}
),

employee_bill_lines as (
    select
        'employee_bill_line_item' as source_table,
        id as transaction_id,
        new_parent_id_no as transaction_no,
        bill_invoice_date::date as transaction_date,
        amount as transaction_amount,
        -- this form carries neither a gross nor a tax figure
        null::numeric as gross_amount,
        null::numeric as tax_amount,
        expense_category,
        expense_location,
        merchant_name as vendor_name,
        new_parent_id_bhumi_name as bhumi_name,
        event,
        null::text as source_status,
        null::text as payment_status,
        description1 as description,
        new_parent_id_entity as entity_name,
        donor_name,
        project,
        donor_project_name,
        donor_budget_line_item
    from {{ ref('employee_bill_line_item_report_stg') }}
),

bills as (
    select * from approved_bills
    union all
    select * from employee_bill_lines
),

bills_keyed as (
    select
        *,
        -- indian FY, april-march. the macro reads an implausible date as unknown rather
        -- than deriving a bogus year.
        {{ financial_year_start_year('transaction_date') }} as financial_year_start_year,
        nullif(regexp_replace(btrim(entity_name), '\s+', ' ', 'g'), '') as key_entity_name,
        nullif(regexp_replace(btrim(donor_name), '\s+', ' ', 'g'), '') as key_donor_name,
        nullif(regexp_replace(btrim(project), '\s+', ' ', 'g'), '') as key_project,
        nullif(regexp_replace(btrim(donor_project_name), '\s+', ' ', 'g'), '') as key_donor_project_name,
        nullif(regexp_replace(btrim(donor_budget_line_item), '\s+', ' ', 'g'), '') as key_donor_budget_line_item
    from bills
),

bills_ready as (
    select
        *,
        -- rendered in the same text format the master stores, so the two join directly
        {{ financial_year_label('financial_year_start_year', 'zoho_long') }} as key_financial_year
    from bills_keyed
)

select
    -- join keys, taken from whichever side of the full outer join is present
    coalesce(t.key_financial_year, b.key_financial_year) as financial_year,
    coalesce(t.key_entity_name, b.key_entity_name) as entity_name,
    coalesce(t.key_donor_name, b.key_donor_name) as donor_name,
    coalesce(t.key_project, b.key_project) as project,
    coalesce(t.key_donor_project_name, b.key_donor_project_name) as donor_project_name,
    coalesce(t.key_donor_budget_line_item, b.key_donor_budget_line_item) as donor_budget_line_item,
    -- budget line item master
    b.budget_line_item_id,
    b.budget_line_item_code,
    b.budget_master_id,
    b.budget_line_status,
    b.budget_amount,
    b.budget_total_paid,
    b.donation_type,
    b.donation_source,
    b.budget_location,
    b.mou_start_date,
    b.mou_end_date,
    -- bill
    t.source_table,
    t.transaction_id,
    t.transaction_no,
    t.transaction_date,
    t.transaction_amount,
    t.gross_amount,
    t.tax_amount,
    t.expense_category,
    t.expense_location,
    t.vendor_name,
    t.bhumi_name,
    t.event,
    t.source_status,
    t.payment_status,
    t.description
from bills_ready as t
full outer join budget_lines as b
    on
        t.key_financial_year = b.key_financial_year
        and t.key_entity_name = b.key_entity_name
        and t.key_donor_name = b.key_donor_name
        and t.key_project = b.key_project
        and t.key_donor_project_name = b.key_donor_project_name
        and t.key_donor_budget_line_item = b.key_donor_budget_line_item
