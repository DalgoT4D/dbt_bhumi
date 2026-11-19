{{ config(
  materialized='table',
  tags=["finance", "staging"]
) }}

with approved_bills as (
    select
        nullif(btrim("ID"::text), '') as id,
        coalesce(("Tax"::jsonb)->>'Display', ("Tax"::jsonb)->>'zc_display_value', ("Tax"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Tax"::text), ''), '[]'), '{}')) as tax,
        coalesce(("Event"::jsonb)->>'zc_display_value', ("Event"::jsonb)->>'Display', ("Event"::jsonb)->>'Event', ("Event"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Event"::text), ''), '[]'), '{}')) as event,
        case
            when regexp_replace(btrim("Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Amount"::text), '[^0-9.-]', '', 'g')::numeric
        end as amount,
        coalesce(("Project"::jsonb)->>'zc_display_value', ("Project"::jsonb)->>'Project', ("Project"::jsonb)->>'Option', ("Project"::jsonb)->>'Display', ("Project"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Project"::text), ''), '[]'), '{}')) as project,
        case
            when btrim("Added_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Added_Time"::text)::timestamp
            when btrim("Added_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Added_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Added_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}$' then to_timestamp(btrim("Added_Time"::text), 'DD-Mon-YYYY HH24:MI')::timestamp
            when btrim("Added_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Added_Time"::text), 'DD-Mon-YYYY')::timestamp
            when btrim("Added_Time"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_timestamp(btrim("Added_Time"::text), 'DD/MM/YYYY')::timestamp
        end as added_time,
        case
            when regexp_replace(btrim("Tax_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Tax_Amount"::text), '[^0-9.-]', '', 'g')::numeric
        end as tax_amount,
        coalesce(("Old_Project"::jsonb)->>'Option', ("Old_Project"::jsonb)->>'zc_display_value', ("Old_Project"::jsonb)->>'Project', ("Old_Project"::jsonb)->>'Display', ("Old_Project"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Old_Project"::text), ''), '[]'), '{}')) as old_project,
        case
            when regexp_replace(btrim("Tax_Percent"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Tax_Percent"::text), '[^0-9.-]', '', 'g')::numeric
        end as tax_percent,
        coalesce(("Budget_Code1"::jsonb)->>'zc_display_value', ("Budget_Code1"::jsonb)->>'Budget_Code', ("Budget_Code1"::jsonb)->>'Display', ("Budget_Code1"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Budget_Code1"::text), ''), '[]'), '{}')) as budget_code1,
        nullif(btrim("Description1"::text), '') as description1,
        nullif(btrim("Merchant_IFSC"::text), '') as merchant_ifsc,
        coalesce(("Budget_Code_Old"::jsonb)->>'zc_display_value', ("Budget_Code_Old"::jsonb)->>'Budget_Code', ("Budget_Code_Old"::jsonb)->>'Display', ("Budget_Code_Old"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Budget_Code_Old"::text), ''), '[]'), '{}')) as budget_code_old,
        coalesce(("Customer_Details"::jsonb)->>'Company_Name', ("Customer_Details"::jsonb)->>'zc_display_value', ("Customer_Details"::jsonb)->>'Display', ("Customer_Details"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Customer_Details"::text), ''), '[]'), '{}')) as donor_name,
        coalesce(("Expense_Category"::jsonb)->>'Account_Name', ("Expense_Category"::jsonb)->>'zc_display_value', ("Expense_Category"::jsonb)->>'Display', ("Expense_Category"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Expense_Category"::text), ''), '[]'), '{}')) as expense_category,
        case
            when jsonb_typeof("Expense_Location"::jsonb) = 'array'
                then nullif((select string_agg(t.elem->>'zc_display_value', ', ' order by t.ord) from jsonb_array_elements(src."Expense_Location"::jsonb) with ordinality as t (elem, ord)), '')
            else coalesce(("Expense_Location"::jsonb)->>'zc_display_value', ("Expense_Location"::jsonb)->>'Option', ("Expense_Location"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Expense_Location"::text), ''), '[]'), '{}'))
        end as expense_location,
        coalesce(("Donor_Project_Name"::jsonb)->>'Donor_Project_Name', ("Donor_Project_Name"::jsonb)->>'zc_display_value', ("Donor_Project_Name"::jsonb)->>'Display', ("Donor_Project_Name"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Donor_Project_Name"::text), ''), '[]'), '{}')) as donor_project_name,
        coalesce(("New_Parent_ID_Entity"::jsonb)->>'Entity_Name', ("New_Parent_ID_Entity"::jsonb)->>'zc_display_value', ("New_Parent_ID_Entity"::jsonb)->>'Display', ("New_Parent_ID_Entity"::jsonb)->>'Name', nullif(nullif(nullif(btrim("New_Parent_ID_Entity"::text), ''), '[]'), '{}')) as new_parent_id_entity,
        coalesce(("New_Parent_ID_Status"::jsonb)->>'Display', ("New_Parent_ID_Status"::jsonb)->>'zc_display_value', ("New_Parent_ID_Status"::jsonb)->>'Status', ("New_Parent_ID_Status"::jsonb)->>'Name', nullif(nullif(nullif(btrim("New_Parent_ID_Status"::text), ''), '[]'), '{}')) as new_parent_id_status,
        coalesce(("Donor_Budget_Line_Item"::jsonb)->>'Donor_Budget_line_Item', ("Donor_Budget_Line_Item"::jsonb)->>'zc_display_value', ("Donor_Budget_Line_Item"::jsonb)->>'Display', ("Donor_Budget_Line_Item"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Donor_Budget_Line_Item"::text), ''), '[]'), '{}')) as donor_budget_line_item,
        nullif(btrim("New_Parent_ID_Comments"::text), '') as new_parent_id_comments,
        case
            when regexp_replace(btrim("New_Parent_ID_Sub_Total"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("New_Parent_ID_Sub_Total"::text), '[^0-9.-]', '', 'g')::numeric
        end as new_parent_id_sub_total,
        case
            when regexp_replace(btrim("Gross_Amount_Tax_applied"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Gross_Amount_Tax_applied"::text), '[^0-9.-]', '', 'g')::numeric
        end as gross_amount_tax_applied,
        coalesce(("New_Parent_ID_Bhumi_Name"::jsonb)->>'Name', ("New_Parent_ID_Bhumi_Name"::jsonb)->>'zc_display_value', ("New_Parent_ID_Bhumi_Name"::jsonb)->>'Display', nullif(nullif(nullif(btrim("New_Parent_ID_Bhumi_Name"::text), ''), '[]'), '{}')) as new_parent_id_bhumi_name,
        case
            when regexp_replace(btrim("New_Parent_ID_TDS_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("New_Parent_ID_TDS_Amount"::text), '[^0-9.-]', '', 'g')::numeric
        end as new_parent_id_tds_amount,
        nullif(btrim("New_Parent_ID_Voucher_No"::text), '') as new_parent_id_voucher_no,
        coalesce(("New_Parent_ID_Vendor_Name"::jsonb)->>'Company_Name', ("New_Parent_ID_Vendor_Name"::jsonb)->>'zc_display_value', ("New_Parent_ID_Vendor_Name"::jsonb)->>'Vendor_Name', ("New_Parent_ID_Vendor_Name"::jsonb)->>'Display', ("New_Parent_ID_Vendor_Name"::jsonb)->>'Name', nullif(nullif(nullif(btrim("New_Parent_ID_Vendor_Name"::text), ''), '[]'), '{}')) as new_parent_id_vendor_name,
        case
            when regexp_replace(btrim("New_Parent_ID_Total_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("New_Parent_ID_Total_Amount"::text), '[^0-9.-]', '', 'g')::numeric
        end as new_parent_id_total_amount,
        nullif(btrim("Budget_Code_Old_Budget_Code"::text), '') as budget_code_old_budget_code,
        nullif(btrim("New_Parent_ID_Payment_Status"::text), '') as new_parent_id_payment_status,
        case
            when jsonb_typeof("New_Parent_ID_Expense_Location"::jsonb) = 'array'
                then nullif((select string_agg(t.elem->>'zc_display_value', ', ' order by t.ord) from jsonb_array_elements(src."New_Parent_ID_Expense_Location"::jsonb) with ordinality as t (elem, ord)), '')
            else coalesce(("New_Parent_ID_Expense_Location"::jsonb)->>'zc_display_value', ("New_Parent_ID_Expense_Location"::jsonb)->>'Option', ("New_Parent_ID_Expense_Location"::jsonb)->>'Name', nullif(nullif(nullif(btrim("New_Parent_ID_Expense_Location"::text), ''), '[]'), '{}'))
        end as new_parent_id_expense_location,
        coalesce(("New_Parent_ID_Source_of_Supply"::jsonb)->>'Display', ("New_Parent_ID_Source_of_Supply"::jsonb)->>'zc_display_value', ("New_Parent_ID_Source_of_Supply"::jsonb)->>'Name', nullif(nullif(nullif(btrim("New_Parent_ID_Source_of_Supply"::text), ''), '[]'), '{}')) as new_parent_id_source_of_supply,
        case
            when btrim("New_Parent_ID_Bill_Invoice_Date"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("New_Parent_ID_Bill_Invoice_Date"::text)::timestamp
            when btrim("New_Parent_ID_Bill_Invoice_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("New_Parent_ID_Bill_Invoice_Date"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("New_Parent_ID_Bill_Invoice_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("New_Parent_ID_Bill_Invoice_Date"::text), 'DD-Mon-YYYY')::timestamp
            when btrim("New_Parent_ID_Bill_Invoice_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_timestamp(btrim("New_Parent_ID_Bill_Invoice_Date"::text), 'DD/MM/YYYY')::timestamp
        end as new_parent_id_bill_invoice_date,
        case
            when btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text)::timestamp
            when btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text), 'DD-Mon-YYYY')::timestamp
            when btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_timestamp(btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text), 'DD/MM/YYYY')::timestamp
        end as new_parent_id_zoho_books_bill_date,
        coalesce(("New_Parent_ID_Destination_of_Supply"::jsonb)->>'Display', ("New_Parent_ID_Destination_of_Supply"::jsonb)->>'zc_display_value', ("New_Parent_ID_Destination_of_Supply"::jsonb)->>'Name', nullif(nullif(nullif(btrim("New_Parent_ID_Destination_of_Supply"::text), ''), '[]'), '{}')) as new_parent_id_destination_of_supply,
        nullif(btrim("New_Parent_ID_Bill_Invoice_Attachment"::text), '') as new_parent_id_bill_invoice_attachment,
        case
            when regexp_replace(btrim("Budget_Line_Item_ID_For_Bill_Budget_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Budget_Line_Item_ID_For_Bill_Budget_Amount"::text), '[^0-9.-]', '', 'g')::numeric
        end as budget_line_item_id_for_bill_budget_amount
    from {{ source('finance_raw_data', 'All_Approved_Bills') }} as src
)

select *
from approved_bills
