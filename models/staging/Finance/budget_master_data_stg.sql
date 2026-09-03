{{ config(
  materialized='table',
  tags=["finance", "staging"]
) }}

with budget_masters as (
    select
        nullif(btrim("ID"::text), '') as id,
        nullif(btrim("Series"::text), '') as series,
        coalesce(("Donor_Name"::jsonb)->>'zc_display_value', ("Donor_Name"::jsonb)->>'Name', nullif(btrim("Donor_Name"::text), '')) as donor_name,
        coalesce(("Entity_Name"::jsonb)->>'zc_display_value', ("Entity_Name"::jsonb)->>'Name', nullif(btrim("Entity_Name"::text), '')) as entity_name,
        case
            when btrim("MOU_End_Date"::text) ~ '^\d{4}-\d{2}-\d{2}$' then btrim("MOU_End_Date"::text)::date
            when btrim("MOU_End_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(btrim("MOU_End_Date"::text), 'DD/MM/YYYY')
            when btrim("MOU_End_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(btrim("MOU_End_Date"::text), 'DD-Mon-YYYY')
        end as mou_end_date,
        case
            when regexp_replace(btrim("Budget_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Budget_Amount"::text), '[^0-9.-]', '', 'g')::numeric
        end as budget_amount,
        nullif(btrim("Donation_Type"::text), '') as donation_type,
        coalesce(("Financial_Year"::jsonb)->>'zc_display_value', ("Financial_Year"::jsonb)->>'Name', nullif(btrim("Financial_Year"::text), '')) as financial_year,
        case
            when btrim("MOU_Start_Date"::text) ~ '^\d{4}-\d{2}-\d{2}$' then btrim("MOU_Start_Date"::text)::date
            when btrim("MOU_Start_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(btrim("MOU_Start_Date"::text), 'DD/MM/YYYY')
            when btrim("MOU_Start_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(btrim("MOU_Start_Date"::text), 'DD-Mon-YYYY')
        end as mou_start_date,
        coalesce(("Budget_Line_Items"::jsonb)->>'zc_display_value', ("Budget_Line_Items"::jsonb)->>'Name', nullif(btrim("Budget_Line_Items"::text), '')) as budget_line_items
    from {{ source('finance_raw_data', 'all_budget_masters') }}
)

select *
from budget_masters
