{{ config(
  materialized='table',
  tags=["finance", "staging"]
) }}

with budget_line_items as (
    select
        nullif(btrim("ID"::text), '') as id,
        nullif(btrim("Series"::text), '') as series,
        nullif(btrim("Status"::text), '') as status,
        coalesce(("Project"::jsonb)->>'zc_display_value', ("Project"::jsonb)->>'Project', ("Project"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Project"::text), ''), '[]'), '{}')) as project,
        nullif(btrim("Comments"::text), '') as comments,
        case
            when jsonb_typeof("Location"::jsonb) = 'array'
                then nullif((select string_agg(t.elem->>'zc_display_value', ', ' order by t.ord) from jsonb_array_elements(src."Location"::jsonb) with ordinality as t (elem, ord)), '')
            else coalesce(("Location"::jsonb)->>'zc_display_value', ("Location"::jsonb)->>'Option', ("Location"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Location"::text), ''), '[]'), '{}'))
        end as location,
        coalesce(("Donor_Name"::jsonb)->>'zc_display_value', ("Donor_Name"::jsonb)->>'Donor_Name', ("Donor_Name"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Donor_Name"::text), ''), '[]'), '{}')) as donor_name,
        case
            when regexp_replace(btrim("Total_Paid"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Total_Paid"::text), '[^0-9.-]', '', 'g')::numeric
        end as total_paid,
        coalesce(("Entity_Name"::jsonb)->>'zc_display_value', ("Entity_Name"::jsonb)->>'Entity_Name', ("Entity_Name"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Entity_Name"::text), ''), '[]'), '{}')) as entity_name,
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
        coalesce(("Financial_Year"::jsonb)->>'zc_display_value', ("Financial_Year"::jsonb)->>'Financial_Year', ("Financial_Year"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Financial_Year"::text), ''), '[]'), '{}')) as financial_year,
        case
            when btrim("MOU_Start_Date"::text) ~ '^\d{4}-\d{2}-\d{2}$' then btrim("MOU_Start_Date"::text)::date
            when btrim("MOU_Start_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(btrim("MOU_Start_Date"::text), 'DD/MM/YYYY')
            when btrim("MOU_Start_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(btrim("MOU_Start_Date"::text), 'DD-Mon-YYYY')
        end as mou_start_date,
        nullif(btrim("Donation_Source"::text), '') as donation_source,
        coalesce(("Budget_Master_ID"::jsonb)->>'ID', ("Budget_Master_ID"::jsonb)->>'zc_display_value', nullif(nullif(nullif(btrim("Budget_Master_ID"::text), ''), '[]'), '{}')) as budget_master_id,
        case
            when regexp_replace(btrim("Tolerance_Balance"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Tolerance_Balance"::text), '[^0-9.-]', '', 'g')::numeric
        end as tolerance_balance,
        coalesce(("Donor_Project_Name"::jsonb)->>'Donor_Project_Name', ("Donor_Project_Name"::jsonb)->>'zc_display_value', ("Donor_Project_Name"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Donor_Project_Name"::text), ''), '[]'), '{}')) as donor_project_name,
        case
            when regexp_replace(btrim("Balance_W_O_Payable"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Balance_W_O_Payable"::text), '[^0-9.-]', '', 'g')::numeric
        end as balance_w_o_payable,
        case
            when regexp_replace(btrim("Utilization_Percent"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Utilization_Percent"::text), '[^0-9.-]', '', 'g')::numeric
        end as utilization_percent,
        case
            when regexp_replace(btrim("Balance_With_Payable"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Balance_With_Payable"::text), '[^0-9.-]', '', 'g')::numeric
        end as balance_with_payable,
        coalesce(("Donor_Budget_Line_Item"::jsonb)->>'Donor_Budget_line_Item', ("Donor_Budget_Line_Item"::jsonb)->>'zc_display_value', ("Donor_Budget_Line_Item"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Donor_Budget_Line_Item"::text), ''), '[]'), '{}')) as donor_budget_line_item,
        nullif(btrim("Project_Project_Manager_1_Email"::text), '') as project_project_manager_1_email,
        nullif(btrim("Project_Project_Manager_2_Email"::text), '') as project_project_manager_2_email
    from {{ source('finance_raw_data', 'Budget_Line_Item_Master_Report') }} as src
)

select *
from budget_line_items
