{{ config(
  materialized='table',
  tags=["finance", "staging"]
) }}

with project_utilisations as (
    select
        nullif(btrim("ID"::text), '') as id,
        nullif(btrim("No"::text), '') as no,
        case
            when regexp_replace(btrim("GST"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("GST"::text), '[^0-9.-]', '', 'g')::numeric
        end as gst,
        coalesce(("TDS"::jsonb)->>'zc_display_value', ("TDS"::jsonb)->>'Display', ("TDS"::jsonb)->>'Name', nullif(nullif(nullif(btrim("TDS"::text), ''), '[]'), '{}')) as tds,
        coalesce(("Event"::jsonb)->>'zc_display_value', ("Event"::jsonb)->>'Display', ("Event"::jsonb)->>'Event', ("Event"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Event"::text), ''), '[]'), '{}')) as event,
        nullif(btrim("ZB_ID"::text), '') as zb_id,
        coalesce(("Entity"::jsonb)->>'zc_display_value', ("Entity"::jsonb)->>'Display', ("Entity"::jsonb)->>'Entity_Name', ("Entity"::jsonb)->>'Entity', ("Entity"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Entity"::text), ''), '[]'), '{}')) as entity,
        coalesce(("Status"::jsonb)->>'zc_display_value', ("Status"::jsonb)->>'Display', ("Status"::jsonb)->>'Status', ("Status"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Status"::text), ''), '[]'), '{}')) as status,
        coalesce(("Project"::jsonb)->>'zc_display_value', ("Project"::jsonb)->>'Display', ("Project"::jsonb)->>'Project', ("Project"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Project"::text), ''), '[]'), '{}')) as project,
        nullif(btrim("Comments"::text), '') as comments,
        case
            when regexp_replace(btrim("Sub_Total"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Sub_Total"::text), '[^0-9.-]', '', 'g')::numeric
        end as sub_total,
        case
            when btrim("Added_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Added_Time"::text)::timestamp
            when btrim("Added_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Added_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Added_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Added_Time"::text), 'DD-Mon-YYYY')::timestamp
        end as added_time,
        nullif(btrim("Added_User"::text), '') as added_user,
        coalesce(("Donor_Name"::jsonb)->>'zc_display_value', ("Donor_Name"::jsonb)->>'Display', ("Donor_Name"::jsonb)->>'Donor_Name', ("Donor_Name"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Donor_Name"::text), ''), '[]'), '{}')) as donor_name,
        case
            when regexp_replace(btrim("TDS_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("TDS_Amount"::text), '[^0-9.-]', '', 'g')::numeric
        end as tds_amount,
        coalesce(("Type_field"::jsonb)->>'zc_display_value', ("Type_field"::jsonb)->>'Display', ("Type_field"::jsonb)->>'Type_field', ("Type_field"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Type_field"::text), ''), '[]'), '{}')) as type_field,
        nullif(btrim("Voucher_No"::text), '') as voucher_no,
        case
            when regexp_replace(btrim("Amount_Paid"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Amount_Paid"::text), '[^0-9.-]', '', 'g')::numeric
        end as amount_paid,
        coalesce(("Vendor_Name"::jsonb)->>'zc_display_value', ("Vendor_Name"::jsonb)->>'Company_Name', ("Vendor_Name"::jsonb)->>'Display', ("Vendor_Name"::jsonb)->>'Vendor_Name', ("Vendor_Name"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Vendor_Name"::text), ''), '[]'), '{}')) as vendor_name,
        case
            when jsonb_typeof("Approval_Log"::jsonb) = 'array'
                then nullif((select string_agg(t.elem->>'zc_display_value', '; ' order by t.ord) from jsonb_array_elements(src."Approval_Log"::jsonb) with ordinality as t (elem, ord)), '')
            else coalesce(("Approval_Log"::jsonb)->>'zc_display_value', ("Approval_Log"::jsonb)->>'Display', ("Approval_Log"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Approval_Log"::text), ''), '[]'), '{}'))
        end as approval_log,
        nullif(btrim("Pending_with"::text), '') as pending_with,
        case
            when regexp_replace(btrim("Total_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Total_Amount"::text), '[^0-9.-]', '', 'g')::numeric
        end as total_amount,
        case
            when regexp_replace(btrim("Balance_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Balance_Amount"::text), '[^0-9.-]', '', 'g')::numeric
        end as balance_amount,
        nullif(btrim("Payment_Status"::text), '') as payment_status,
        nullif(btrim("Bhumi_Name_Name"::text), '') as bhumi_name_name,
        case
            when btrim("ED_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("ED_Approved_Time"::text)::timestamp
            when btrim("ED_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("ED_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("ED_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("ED_Approved_Time"::text), 'DD-Mon-YYYY')::timestamp
        end as ed_approved_time,
        case
            when btrim("Bill_Invoice_Date"::text) ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Bill_Invoice_Date"::text)::date
            when btrim("Bill_Invoice_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(btrim("Bill_Invoice_Date"::text), 'DD/MM/YYYY')
            when btrim("Bill_Invoice_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(btrim("Bill_Invoice_Date"::text), 'DD-Mon-YYYY')
        end as bill_invoice_date,
        coalesce(("Donor_Project_Name"::jsonb)->>'zc_display_value', ("Donor_Project_Name"::jsonb)->>'Donor_Project_Name', ("Donor_Project_Name"::jsonb)->>'Display', ("Donor_Project_Name"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Donor_Project_Name"::text), ''), '[]'), '{}')) as donor_project_name,
        case
            when btrim("Received_by_Finance"::text) ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Received_by_Finance"::text)::date
            when btrim("Received_by_Finance"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(btrim("Received_by_Finance"::text), 'DD/MM/YYYY')
            when btrim("Received_by_Finance"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(btrim("Received_by_Finance"::text), 'DD-Mon-YYYY')
        end as received_by_finance,
        nullif(btrim("Bhumi_Name_Bank_Name"::text), '') as bhumi_name_bank_name,
        case
            when btrim("Finance_Payment_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Finance_Payment_Time"::text)::timestamp
            when btrim("Finance_Payment_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Finance_Payment_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Finance_Payment_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Finance_Payment_Time"::text), 'DD-Mon-YYYY')::timestamp
        end as finance_payment_time,
        case
            when btrim("Manager_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Manager_Approved_Time"::text)::timestamp
            when btrim("Manager_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Manager_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Manager_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Manager_Approved_Time"::text), 'DD-Mon-YYYY')::timestamp
        end as manager_approved_time,
        case
            when btrim("Director_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Director_Approved_Time"::text)::timestamp
            when btrim("Director_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Director_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Director_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Director_Approved_Time"::text), 'DD-Mon-YYYY')::timestamp
        end as director_approved_time,
        coalesce(("Donor_Budget_Line_Item"::jsonb)->>'zc_display_value', ("Donor_Budget_Line_Item"::jsonb)->>'Donor_Budget_line_Item', ("Donor_Budget_Line_Item"::jsonb)->>'Display', ("Donor_Budget_Line_Item"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Donor_Budget_Line_Item"::text), ''), '[]'), '{}')) as donor_budget_line_item,
        case
            when btrim("No_of_Lead_Days_for_ED"::text) ~ '^-?[0-9]+$' then btrim("No_of_Lead_Days_for_ED"::text)::integer
        end as no_of_lead_days_for_ed,
        case
            when btrim("No_of_Lead_Days_for_FA"::text) ~ '^-?[0-9]+$' then btrim("No_of_Lead_Days_for_FA"::text)::integer
        end as no_of_lead_days_for_fa,
        case
            when btrim("No_of_Lead_Days_for_FM"::text) ~ '^-?[0-9]+$' then btrim("No_of_Lead_Days_for_FM"::text)::integer
        end as no_of_lead_days_for_fm,
        case
            when btrim("No_of_Lead_Days_for_PM1"::text) ~ '^-?[0-9]+$' then btrim("No_of_Lead_Days_for_PM1"::text)::integer
        end as no_of_lead_days_for_pm1,
        case
            when btrim("No_of_Lead_Days_for_PM2"::text) ~ '^-?[0-9]+$' then btrim("No_of_Lead_Days_for_PM2"::text)::integer
        end as no_of_lead_days_for_pm2,
        case
            when btrim("No_of_Lead_Days_for_Manager"::text) ~ '^-?[0-9]+$' then btrim("No_of_Lead_Days_for_Manager"::text)::integer
        end as no_of_lead_days_for_manager,
        case
            when btrim("No_of_Lead_Days_for_Director"::text) ~ '^-?[0-9]+$' then btrim("No_of_Lead_Days_for_Director"::text)::integer
        end as no_of_lead_days_for_director,
        case
            when btrim("Finance_Manager_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Finance_Manager_Approved_Time"::text)::timestamp
            when btrim("Finance_Manager_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Finance_Manager_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Finance_Manager_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Finance_Manager_Approved_Time"::text), 'DD-Mon-YYYY')::timestamp
        end as finance_manager_approved_time,
        case
            when btrim("Finance_Associate_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Finance_Associate_Approved_Time"::text)::timestamp
            when btrim("Finance_Associate_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Finance_Associate_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Finance_Associate_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Finance_Associate_Approved_Time"::text), 'DD-Mon-YYYY')::timestamp
        end as finance_associate_approved_time,
        case
            when btrim("Project_Manager_1_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Project_Manager_1_Approved_Time"::text)::timestamp
            when btrim("Project_Manager_1_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Project_Manager_1_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Project_Manager_1_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Project_Manager_1_Approved_Time"::text), 'DD-Mon-YYYY')::timestamp
        end as project_manager_1_approved_time,
        case
            when btrim("Project_Manager_2_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Project_Manager_2_Approved_Time"::text)::timestamp
            when btrim("Project_Manager_2_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Project_Manager_2_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Project_Manager_2_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Project_Manager_2_Approved_Time"::text), 'DD-Mon-YYYY')::timestamp
        end as project_manager_2_approved_time
    from {{ source('finance_raw_data', 'Project_Utilisation_Summary_Report') }} as src
)

select *
from project_utilisations
