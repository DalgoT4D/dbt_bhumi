{{ config(
  materialized='table',
  tags=["finance", "staging"]
) }}

with vouchers as (
	select
		nullif(btrim("ID"::text), '') as id,
		nullif(btrim("No"::text), '') as voucher_number,
		nullif(btrim("GST"::text), '') as gst,
		coalesce(("TDS"::jsonb)->>'zc_display_value', ("TDS"::jsonb)->>'Name', nullif(btrim("TDS"::text), '')) as tds,
		coalesce(("Class"::jsonb)->>'zc_display_value', ("Class"::jsonb)->>'Name', nullif(btrim("Class"::text), '')) as class_name,
		coalesce(("Event"::jsonb)->>'zc_display_value', ("Event"::jsonb)->>'Name', nullif(btrim("Event"::text), '')) as event_name,
		nullif(btrim("ZB_ID"::text), '') as zb_id,
		coalesce(("Entity"::jsonb)->>'zc_display_value', ("Entity"::jsonb)->>'Name', nullif(btrim("Entity"::text), '')) as entity,
		coalesce(("Status"::jsonb)->>'zc_display_value', ("Status"::jsonb)->>'Name', nullif(btrim("Status"::text), '')) as status,
		coalesce(("Details"::jsonb)->>'zc_display_value', ("Details"::jsonb)->>'Name', nullif(btrim("Details"::text), '')) as details,
		coalesce(("Project"::jsonb)->>'zc_display_value', ("Project"::jsonb)->>'Name', nullif(btrim("Project"::text), '')) as project,
		nullif(btrim("Comments"::text), '') as comments,

		case when regexp_replace(btrim("Sub_Total"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
			then regexp_replace(btrim("Sub_Total"::text), '[^0-9.-]', '', 'g')::numeric end as sub_total,

		case
			when btrim("Added_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Added_Time"::text)::timestamp
			when btrim("Added_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Added_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')
		end as added_time,
		nullif(btrim("Added_User"::text), '') as added_user,
		coalesce(("Bhumi_Name"::jsonb)->>'zc_display_value', ("Bhumi_Name"::jsonb)->>'Name', nullif(btrim("Bhumi_Name"::text), '')) as bhumi_name,

		case
			when btrim("Date_field"::text) ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Date_field"::text)::date
			when btrim("Date_field"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(btrim("Date_field"::text), 'DD/MM/YYYY')
			when btrim("Date_field"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(btrim("Date_field"::text), 'DD-Mon-YYYY')
		end as date_field,
		coalesce(("Donor_Name"::jsonb)->>'zc_display_value', ("Donor_Name"::jsonb)->>'Name', nullif(btrim("Donor_Name"::text), '')) as donor_name,
		case when regexp_replace(btrim("TDS_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
			then regexp_replace(btrim("TDS_Amount"::text), '[^0-9.-]', '', 'g')::numeric end as tds_amount,
		coalesce(("Type_field"::jsonb)->>'zc_display_value', ("Type_field"::jsonb)->>'Name', nullif(btrim("Type_field"::text), '')) as type_field,
		case when regexp_replace(btrim("Voucher_No"::text), '[^0-9]', '', 'g') <> '' then nullif(btrim("Voucher_No"::text), '') end as voucher_no,
		case when regexp_replace(btrim("Amount_Paid"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
			then regexp_replace(btrim("Amount_Paid"::text), '[^0-9.-]', '', 'g')::numeric end as amount_paid,
		case when lower(btrim("Is_Imported"::text)) in ('yes', 'y', 'true', '1') then true
			when lower(btrim("Is_Imported"::text)) in ('no', 'n', 'false', '0') then false end as is_imported,
		coalesce(("Old_Project"::jsonb)->>'zc_display_value', ("Old_Project"::jsonb)->>'Name', nullif(btrim("Old_Project"::text), '')) as old_project,
		coalesce(("Vendor_Name"::jsonb)->>'zc_display_value', ("Vendor_Name"::jsonb)->>'Name', nullif(btrim("Vendor_Name"::text), '')) as vendor_name,
		nullif(btrim("Approval_Log"::text), '') as approval_log,

		case when btrim("ED_Lead_Time"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("ED_Lead_Time"::text)::numeric end as ed_lead_time,
		nullif(btrim("Journal_ZBID"::text), '') as journal_zbid,
		nullif(btrim("Pending_with"::text), '') as pending_with,
		case when regexp_replace(btrim("Total_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
			then regexp_replace(btrim("Total_Amount"::text), '[^0-9.-]', '', 'g')::numeric end as total_amount,
		case
			when btrim("Modified_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Modified_Time"::text)::timestamp
			when btrim("Modified_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Modified_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')
		end as modified_time,
		nullif(btrim("Modified_User"::text), '') as modified_user,
		nullif(btrim("Report_Filter"::text), '') as report_filter,
		case when regexp_replace(btrim("Balance_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
			then regexp_replace(btrim("Balance_Amount"::text), '[^0-9.-]', '', 'g')::numeric end as balance_amount,
		nullif(btrim("Payment_Status"::text), '') as payment_status,
		nullif(btrim("Project_import"::text), '') as project_import,

		case
			when btrim("ED_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("ED_Approved_Time"::text)::timestamp
			when btrim("ED_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("ED_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')
		end as ed_approved_time,
		coalesce(("Expense_Location"::jsonb)->>'zc_display_value', ("Expense_Location"::jsonb)->>'Name', nullif(btrim("Expense_Location"::text), '')) as expense_location,
		nullif(btrim("Old_Approval_Log"::text), '') as old_approval_log,

		case
			when btrim("Bill_Invoice_Date"::text) ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Bill_Invoice_Date"::text)::date
			when btrim("Bill_Invoice_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(btrim("Bill_Invoice_Date"::text), 'DD/MM/YYYY')
			when btrim("Bill_Invoice_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(btrim("Bill_Invoice_Date"::text), 'DD-Mon-YYYY')
		end as bill_invoice_date,

		case when btrim("Manager_Lead_Time"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("Manager_Lead_Time"::text)::numeric end as manager_lead_time,
		case when btrim("Director_Lead_Time"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("Director_Lead_Time"::text)::numeric end as director_lead_time,
		coalesce(("Donor_Project_Name"::jsonb)->>'zc_display_value', ("Donor_Project_Name"::jsonb)->>'Name', nullif(btrim("Donor_Project_Name"::text), '')) as donor_project_name,
		nullif(btrim("Finance_List_Email"::text), '') as finance_list_email,
		case when btrim("Requestor_Lead_Time"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("Requestor_Lead_Time"::text)::numeric end as requestor_lead_time,
		case when btrim("Authoriser_Lead_Time"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("Authoriser_Lead_Time"::text)::numeric end as authoriser_lead_time,
		nullif(btrim("Bhumi_Name_Bank_Name"::text), '') as bhumi_name_bank_name,
		case when btrim("Finance_Payment_Time"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("Finance_Payment_Time"::text)::numeric end as finance_payment_time,
		case when btrim("Requestor_Lead_Time1"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("Requestor_Lead_Time1"::text)::numeric end as requestor_lead_time1,
		case when btrim("Total_Number_of_Days"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("Total_Number_of_Days"::text)::numeric end as total_number_of_days,

		case
			when btrim("Zoho_Books_Bill_Date"::text) ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Zoho_Books_Bill_Date"::text)::date
			when btrim("Zoho_Books_Bill_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(btrim("Zoho_Books_Bill_Date"::text), 'DD/MM/YYYY')
			when btrim("Zoho_Books_Bill_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(btrim("Zoho_Books_Bill_Date"::text), 'DD-Mon-YYYY')
		end as zoho_books_bill_date,

		case when btrim("Manager_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Manager_Approved_Time"::text)::timestamp end as manager_approved_time,
		nullif(btrim("Approver_Button_Enable"::text), '') as approver_button_enable,
		case when btrim("Director_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Director_Approved_Time"::text)::timestamp end as director_approved_time,
		coalesce(("Donor_Budget_Line_Item"::jsonb)->>'zc_display_value', ("Donor_Budget_Line_Item"::jsonb)->>'Name', nullif(btrim("Donor_Budget_Line_Item"::text), '')) as donor_budget_line_item,
		case when btrim("No_of_Lead_Days_for_ED"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("No_of_Lead_Days_for_ED"::text)::numeric end as no_of_lead_days_for_ed,
		case when btrim("No_of_Lead_Days_for_FM"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("No_of_Lead_Days_for_FM"::text)::numeric end as no_of_lead_days_for_fm,
		nullif(btrim("Bill_Invoice_Attachment"::text), '') as bill_invoice_attachment,
		case when btrim("No_of_Lead_Days_for_PM1"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("No_of_Lead_Days_for_PM1"::text)::numeric end as no_of_lead_days_for_pm1,
		case when btrim("No_of_Lead_Days_for_PM2"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("No_of_Lead_Days_for_PM2"::text)::numeric end as no_of_lead_days_for_pm2,
		coalesce(("Vendor_Name_Entity_type"::jsonb)->>'zc_display_value', ("Vendor_Name_Entity_type"::jsonb)->>'Name', nullif(btrim("Vendor_Name_Entity_type"::text), '')) as vendor_name_entity_type,
		case when btrim("Number_of_Days_Requestor"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("Number_of_Days_Requestor"::text)::numeric end as number_of_days_requestor,
		case when btrim("No_of_Lead_Days_for_Manager"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("No_of_Lead_Days_for_Manager"::text)::numeric end as no_of_lead_days_for_manager,
		case when btrim("No_of_Lead_Days_for_Director"::text) ~ '^\d+(\.[0-9]+)?$' then btrim("No_of_Lead_Days_for_Director"::text)::numeric end as no_of_lead_days_for_director,
		case when btrim("Finance_Manager_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Finance_Manager_Approved_Time"::text)::timestamp end as finance_manager_approved_time,
		case when btrim("Finance_Associate_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Finance_Associate_Approved_Time"::text)::timestamp end as finance_associate_approved_time,
		case when btrim("Project_Manager_1_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Project_Manager_1_Approved_Time"::text)::timestamp end as project_manager_1_approved_time,
		case when btrim("Project_Manager_2_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Project_Manager_2_Approved_Time"::text)::timestamp end as project_manager_2_approved_time
	from {{ source('finance_raw_data', 'all_vouchers_new') }}
)

select *
from vouchers
