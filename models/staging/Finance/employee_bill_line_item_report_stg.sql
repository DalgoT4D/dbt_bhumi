{{ config(
  materialized='table',
  tags=["finance", "staging"]
) }}

with employee_bill_line_items as (
    select
        nullif(btrim("ID"::text), '') as id,
        coalesce(("Event"::jsonb)->>'zc_display_value', ("Event"::jsonb)->>'Display', ("Event"::jsonb)->>'Event', ("Event"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Event"::text), ''), '[]'), '{}')) as event,
        nullif(btrim("ZB_ID"::text), '') as zb_id,
        case
            when regexp_replace(btrim("Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Amount"::text), '[^0-9.-]', '', 'g')::numeric
        end as amount,
        coalesce(("Project"::jsonb)->>'zc_display_value', ("Project"::jsonb)->>'Project', ("Project"::jsonb)->>'Display', ("Project"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Project"::text), ''), '[]'), '{}')) as project,
        coalesce(("Merchant"::jsonb)->>'zc_display_value', ("Merchant"::jsonb)->>'Merchant_Name', ("Merchant"::jsonb)->>'Display', ("Merchant"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Merchant"::text), ''), '[]'), '{}')) as merchant,
        nullif(btrim("Added_User"::text), '') as added_user,
        nullif(btrim("Budget_Code"::text), '') as budget_code,
        coalesce(("Budget_Code1"::jsonb)->>'zc_display_value', ("Budget_Code1"::jsonb)->>'Budget_Code', ("Budget_Code1"::jsonb)->>'Display', ("Budget_Code1"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Budget_Code1"::text), ''), '[]'), '{}')) as budget_code1,
        nullif(btrim("Description1"::text), '') as description1,
        coalesce(("Expense_Group"::jsonb)->>'Account_Group_Name', ("Expense_Group"::jsonb)->>'zc_display_value', ("Expense_Group"::jsonb)->>'Display', ("Expense_Group"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Expense_Group"::text), ''), '[]'), '{}')) as expense_group,
        nullif(btrim("Merchant_Name"::text), '') as merchant_name,
        nullif(btrim("Bill_Invoice_No"::text), '') as bill_invoice_no,
        coalesce(("Budget_Code_Old"::jsonb)->>'zc_display_value', ("Budget_Code_Old"::jsonb)->>'Budget_Code', ("Budget_Code_Old"::jsonb)->>'Display', ("Budget_Code_Old"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Budget_Code_Old"::text), ''), '[]'), '{}')) as budget_code_old,
        coalesce(("Customer_Details"::jsonb)->>'Company_Name', ("Customer_Details"::jsonb)->>'zc_display_value', ("Customer_Details"::jsonb)->>'Display', ("Customer_Details"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Customer_Details"::text), ''), '[]'), '{}')) as donor_name,
        coalesce(("Expense_Category"::jsonb)->>'Account_Name', ("Expense_Category"::jsonb)->>'zc_display_value', ("Expense_Category"::jsonb)->>'Display', ("Expense_Category"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Expense_Category"::text), ''), '[]'), '{}')) as expense_category,
        case
            when jsonb_typeof("Expense_Location"::jsonb) = 'array'
                then nullif((select string_agg(t.elem->>'zc_display_value', ', ' order by t.ord) from jsonb_array_elements(src."Expense_Location"::jsonb) with ordinality as t (elem, ord)), '')
            else coalesce(("Expense_Location"::jsonb)->>'zc_display_value', ("Expense_Location"::jsonb)->>'Option', ("Expense_Location"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Expense_Location"::text), ''), '[]'), '{}'))
        end as expense_location,
        nullif(btrim("New_Parent_ID_No"::text), '') as new_parent_id_no,
        case
            when btrim("Bill_Invoice_Date"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Bill_Invoice_Date"::text)::timestamp
            when btrim("Bill_Invoice_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Bill_Invoice_Date"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Bill_Invoice_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Bill_Invoice_Date"::text), 'DD-Mon-YYYY')::timestamp
            when btrim("Bill_Invoice_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_timestamp(btrim("Bill_Invoice_Date"::text), 'DD/MM/YYYY')::timestamp
        end as bill_invoice_date,
        coalesce(("Donor_Project_Name"::jsonb)->>'Donor_Project_Name', ("Donor_Project_Name"::jsonb)->>'zc_display_value', ("Donor_Project_Name"::jsonb)->>'Display', ("Donor_Project_Name"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Donor_Project_Name"::text), ''), '[]'), '{}')) as donor_project_name,
        coalesce(("New_Parent_ID_Entity"::jsonb)->>'Entity_Name', ("New_Parent_ID_Entity"::jsonb)->>'zc_display_value', ("New_Parent_ID_Entity"::jsonb)->>'Display', ("New_Parent_ID_Entity"::jsonb)->>'Name', nullif(nullif(nullif(btrim("New_Parent_ID_Entity"::text), ''), '[]'), '{}')) as new_parent_id_entity,
        coalesce(("Donor_Budget_Line_Item"::jsonb)->>'Donor_Budget_line_Item', ("Donor_Budget_Line_Item"::jsonb)->>'zc_display_value', ("Donor_Budget_Line_Item"::jsonb)->>'Display', ("Donor_Budget_Line_Item"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Donor_Budget_Line_Item"::text), ''), '[]'), '{}')) as donor_budget_line_item,
        coalesce(("New_Parent_ID_Bhumi_Name"::jsonb)->>'Name', ("New_Parent_ID_Bhumi_Name"::jsonb)->>'zc_display_value', ("New_Parent_ID_Bhumi_Name"::jsonb)->>'Display', nullif(nullif(nullif(btrim("New_Parent_ID_Bhumi_Name"::text), ''), '[]'), '{}')) as new_parent_id_bhumi_name,
        case
            when regexp_replace(btrim("Budget_Line_Item_ID_For_Bill_Budget_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Budget_Line_Item_ID_For_Bill_Budget_Amount"::text), '[^0-9.-]', '', 'g')::numeric
        end as budget_line_item_id_for_bill_budget_amount
    from {{ source('finance_raw_data', 'Employee_Bill_Line_Item_Report') }} as src
)

select *
from employee_bill_line_items
