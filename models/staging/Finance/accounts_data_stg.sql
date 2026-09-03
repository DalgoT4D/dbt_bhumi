{{ config(
  materialized='table',
  tags=["finance", "staging"]
) }}

with accounts as (
	select
		nullif(btrim("ID"::text), '') as id,
		nullif(btrim("HSN"::text), '') as hsn,
		case when regexp_replace(btrim("Qty"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
			then regexp_replace(btrim("Qty"::text), '[^0-9.-]', '', 'g')::numeric end as qty,
		coalesce(("Tax"::jsonb)->>'zc_display_value', ("Tax"::jsonb)->>'Name', nullif(btrim("Tax"::text), '')) as tax,
		coalesce(("Class"::jsonb)->>'zc_display_value', ("Class"::jsonb)->>'Name', nullif(btrim("Class"::text), '')) as class_name,
		nullif(btrim("ZB_ID"::text), '') as zb_id,
		case when regexp_replace(btrim("Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
			then regexp_replace(btrim("Amount"::text), '[^0-9.-]', '', 'g')::numeric end as amount,
		coalesce(("Status"::jsonb)->>'zc_display_value', ("Status"::jsonb)->>'Name', nullif(btrim("Status"::text), '')) as status,
		case when regexp_replace(btrim("Discount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
			then regexp_replace(btrim("Discount"::text), '[^0-9.-]', '', 'g')::numeric end as discount,
		coalesce(("Merchant"::jsonb)->>'zc_display_value', ("Merchant"::jsonb)->>'Name', nullif(btrim("Merchant"::text), '')) as merchant,
		coalesce(("Parent_ID"::jsonb)->>'zc_display_value', ("Parent_ID"::jsonb)->>'Name', nullif(btrim("Parent_ID"::text), '')) as parent_id,
		case
			when btrim("Added_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Added_Time"::text)::timestamp
			when btrim("Added_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Added_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')
		end as added_time,
		nullif(btrim("Attachment"::text), '') as attachment,
		case when regexp_replace(btrim("Net_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
			then regexp_replace(btrim("Net_Amount"::text), '[^0-9.-]', '', 'g')::numeric end as net_amount,
		nullif(btrim("Budget_Code"::text), '') as budget_code,
		coalesce(("Old_Project"::jsonb)->>'zc_display_value', ("Old_Project"::jsonb)->>'Name', nullif(btrim("Old_Project"::text), '')) as old_project,
		coalesce(("Budget_Code1"::jsonb)->>'zc_display_value', ("Budget_Code1"::jsonb)->>'Name', nullif(btrim("Budget_Code1"::text), '')) as budget_code1,
		nullif(btrim("Description1"::text), '') as description1,
		coalesce(("Expense_Group"::jsonb)->>'zc_display_value', ("Expense_Group"::jsonb)->>'Name', nullif(btrim("Expense_Group"::text), '')) as expense_group,
		nullif(btrim("Merchant_Name"::text), '') as merchant_name,
		coalesce(("New_Parent_ID"::jsonb)->>'zc_display_value', ("New_Parent_ID"::jsonb)->>'Name', nullif(btrim("New_Parent_ID"::text), '')) as new_parent_id,
		nullif(btrim("Parent_Bill_ID"::text), '') as parent_bill_id,
		nullif(btrim("Accounts_Concat"::text), '') as accounts_concat,
		nullif(btrim("Bill_Invoice_No"::text), '') as bill_invoice_no,
		coalesce(("Budget_Code_Old"::jsonb)->>'zc_display_value', ("Budget_Code_Old"::jsonb)->>'Name', nullif(btrim("Budget_Code_Old"::text), '')) as budget_code_old,
		coalesce(("Customer_Details"::jsonb)->>'zc_display_value', ("Customer_Details"::jsonb)->>'Name', nullif(btrim("Customer_Details"::text), '')) as customer_details,
		case
			when btrim("Bill_Invoice_Date"::text) ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Bill_Invoice_Date"::text)::date
			when btrim("Bill_Invoice_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(btrim("Bill_Invoice_Date"::text), 'DD/MM/YYYY')
			when btrim("Bill_Invoice_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(btrim("Bill_Invoice_Date"::text), 'DD-Mon-YYYY')
		end as bill_invoice_date,
		coalesce(("Old_Bi_Directional_ID"::jsonb)->>'zc_display_value', ("Old_Bi_Directional_ID"::jsonb)->>'Name', nullif(btrim("Old_Bi_Directional_ID"::text), '')) as old_bi_directional_id,
		coalesce(("Donor_Budget_Line_Item"::jsonb)->>'zc_display_value', ("Donor_Budget_Line_Item"::jsonb)->>'Name', nullif(btrim("Donor_Budget_Line_Item"::text), '')) as donor_budget_line_item,
		coalesce(("New_Parent_ID_Type_field"::jsonb)->>'zc_display_value', ("New_Parent_ID_Type_field"::jsonb)->>'Name', nullif(btrim("New_Parent_ID_Type_field"::text), '')) as new_parent_id_type_field,
		case
			when btrim("New_Parent_ID_Bill_Invoice_Date"::text) ~ '^\d{4}-\d{2}-\d{2}$' then btrim("New_Parent_ID_Bill_Invoice_Date"::text)::date
			when btrim("New_Parent_ID_Bill_Invoice_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(btrim("New_Parent_ID_Bill_Invoice_Date"::text), 'DD/MM/YYYY')
			when btrim("New_Parent_ID_Bill_Invoice_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(btrim("New_Parent_ID_Bill_Invoice_Date"::text), 'DD-Mon-YYYY')
		end as new_parent_id_bill_invoice_date,
		case
			when btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text) ~ '^\d{4}-\d{2}-\d{2}$' then btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text)::date
			when btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text), 'DD/MM/YYYY')
			when btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(btrim("New_Parent_ID_Zoho_Books_Bill_Date"::text), 'DD-Mon-YYYY')
		end as new_parent_id_zoho_books_bill_date
	from {{ source('finance_raw_data', 'all_accounts') }}
)

select *
from accounts
