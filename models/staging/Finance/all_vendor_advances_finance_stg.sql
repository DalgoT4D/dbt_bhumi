{{ config(
  materialized='table',
  tags=["finance", "staging"]
) }}

with vendor_advances as (
    select
        nullif(btrim("ID"::text), '') as id,
        nullif(btrim("No"::text), '') as no,
        nullif(btrim("GST"::text), '') as gst,
        coalesce(("Event"::jsonb)->>'Event_Name', ("Event"::jsonb)->>'zc_display_value', ("Event"::jsonb)->>'Display', ("Event"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Event"::text), ''), '[]'), '{}')) as event,
        coalesce(("Entity"::jsonb)->>'Entity_Name', ("Entity"::jsonb)->>'zc_display_value', ("Entity"::jsonb)->>'Display', ("Entity"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Entity"::text), ''), '[]'), '{}')) as entity,
        coalesce(("Status"::jsonb)->>'Display', ("Status"::jsonb)->>'zc_display_value', ("Status"::jsonb)->>'Status', ("Status"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Status"::text), ''), '[]'), '{}')) as status,
        coalesce(("Project"::jsonb)->>'Project', ("Project"::jsonb)->>'zc_display_value', ("Project"::jsonb)->>'Option', ("Project"::jsonb)->>'Display', ("Project"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Project"::text), ''), '[]'), '{}')) as project,
        nullif(btrim("Comments"::text), '') as comments,
        case
            when regexp_replace(btrim("Sub_Total"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Sub_Total"::text), '[^0-9.-]', '', 'g')::numeric
        end as sub_total,
        case
            when btrim("Added_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Added_Time"::text)::timestamp
            when btrim("Added_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Added_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Added_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}$' then to_timestamp(btrim("Added_Time"::text), 'DD-Mon-YYYY HH24:MI')::timestamp
            when btrim("Added_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Added_Time"::text), 'DD-Mon-YYYY')::timestamp
            when btrim("Added_Time"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_timestamp(btrim("Added_Time"::text), 'DD/MM/YYYY')::timestamp
        end as added_time,
        nullif(btrim("Added_User"::text), '') as added_user,
        coalesce(("Bhumi_Name"::jsonb)->>'Name', ("Bhumi_Name"::jsonb)->>'zc_display_value', ("Bhumi_Name"::jsonb)->>'Display', nullif(nullif(nullif(btrim("Bhumi_Name"::text), ''), '[]'), '{}')) as bhumi_name,
        case
            when btrim("Date_field"::text) ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Date_field"::text)::date
            when btrim("Date_field"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(btrim("Date_field"::text), 'DD-Mon-YYYY')
            when btrim("Date_field"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(btrim("Date_field"::text), 'DD/MM/YYYY')
        end as date_field,
        coalesce(("Donor_Name"::jsonb)->>'Company_Name', ("Donor_Name"::jsonb)->>'zc_display_value', ("Donor_Name"::jsonb)->>'Donor_Name', ("Donor_Name"::jsonb)->>'Display', ("Donor_Name"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Donor_Name"::text), ''), '[]'), '{}')) as donor_name,
        coalesce(("Type_field"::jsonb)->>'Display', ("Type_field"::jsonb)->>'zc_display_value', ("Type_field"::jsonb)->>'Type_field', ("Type_field"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Type_field"::text), ''), '[]'), '{}')) as type_field,
        case
            when regexp_replace(btrim("Amount_Paid"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Amount_Paid"::text), '[^0-9.-]', '', 'g')::numeric
        end as amount_paid,
        coalesce(("Old_Project"::jsonb)->>'Option', ("Old_Project"::jsonb)->>'zc_display_value', ("Old_Project"::jsonb)->>'Project', ("Old_Project"::jsonb)->>'Display', ("Old_Project"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Old_Project"::text), ''), '[]'), '{}')) as old_project,
        coalesce(("Vendor_Name"::jsonb)->>'Company_Name', ("Vendor_Name"::jsonb)->>'zc_display_value', ("Vendor_Name"::jsonb)->>'Vendor_Name', ("Vendor_Name"::jsonb)->>'Display', ("Vendor_Name"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Vendor_Name"::text), ''), '[]'), '{}')) as vendor_name,
        coalesce(("Budget_Code1"::jsonb)->>'zc_display_value', ("Budget_Code1"::jsonb)->>'Budget_Code', ("Budget_Code1"::jsonb)->>'Display', ("Budget_Code1"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Budget_Code1"::text), ''), '[]'), '{}')) as budget_code1,
        coalesce(("Budget_Code2"::jsonb)->>'zc_display_value', ("Budget_Code2"::jsonb)->>'Budget_Code', ("Budget_Code2"::jsonb)->>'Display', ("Budget_Code2"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Budget_Code2"::text), ''), '[]'), '{}')) as budget_code2,
        case
            when regexp_replace(btrim("Total_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Total_Amount"::text), '[^0-9.-]', '', 'g')::numeric
        end as total_amount,
        case
            when btrim("Modified_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Modified_Time"::text)::timestamp
            when btrim("Modified_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Modified_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Modified_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}$' then to_timestamp(btrim("Modified_Time"::text), 'DD-Mon-YYYY HH24:MI')::timestamp
            when btrim("Modified_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Modified_Time"::text), 'DD-Mon-YYYY')::timestamp
            when btrim("Modified_Time"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_timestamp(btrim("Modified_Time"::text), 'DD/MM/YYYY')::timestamp
        end as modified_time,
        nullif(btrim("Modified_User"::text), '') as modified_user,
        case
            when jsonb_typeof("Report_Filter"::jsonb) = 'array'
                then nullif((select string_agg(coalesce(t.elem->>'Name', t.elem->>'zc_display_value'), ', ' order by t.ord) from jsonb_array_elements(src."Report_Filter"::jsonb) with ordinality as t (elem, ord)), '')
            else coalesce(("Report_Filter"::jsonb)->>'Name', ("Report_Filter"::jsonb)->>'zc_display_value', ("Report_Filter"::jsonb)->>'Display', nullif(nullif(nullif(btrim("Report_Filter"::text), ''), '[]'), '{}'))
        end as report_filter,
        case
            when regexp_replace(btrim("Balance_Amount"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Balance_Amount"::text), '[^0-9.-]', '', 'g')::numeric
        end as balance_amount,
        nullif(btrim("Payment_Status"::text), '') as payment_status,
        case
            when btrim("ED_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("ED_Approved_Time"::text)::timestamp
            when btrim("ED_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("ED_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("ED_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}$' then to_timestamp(btrim("ED_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI')::timestamp
            when btrim("ED_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("ED_Approved_Time"::text), 'DD-Mon-YYYY')::timestamp
            when btrim("ED_Approved_Time"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_timestamp(btrim("ED_Approved_Time"::text), 'DD/MM/YYYY')::timestamp
        end as ed_approved_time,
        case
            when jsonb_typeof("Expense_Location"::jsonb) = 'array'
                then nullif((select string_agg(t.elem->>'zc_display_value', ', ' order by t.ord) from jsonb_array_elements(src."Expense_Location"::jsonb) with ordinality as t (elem, ord)), '')
            else coalesce(("Expense_Location"::jsonb)->>'zc_display_value', ("Expense_Location"::jsonb)->>'Option', ("Expense_Location"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Expense_Location"::text), ''), '[]'), '{}'))
        end as expense_location,
        case
            when btrim("Bill_Invoice_Date"::text) ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Bill_Invoice_Date"::text)::date
            when btrim("Bill_Invoice_Date"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_date(btrim("Bill_Invoice_Date"::text), 'DD-Mon-YYYY')
            when btrim("Bill_Invoice_Date"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_date(btrim("Bill_Invoice_Date"::text), 'DD/MM/YYYY')
        end as bill_invoice_date,
        coalesce(("Donor_Project_Name"::jsonb)->>'Donor_Project_Name', ("Donor_Project_Name"::jsonb)->>'zc_display_value', ("Donor_Project_Name"::jsonb)->>'Display', ("Donor_Project_Name"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Donor_Project_Name"::text), ''), '[]'), '{}')) as donor_project_name,
        case
            when regexp_replace(btrim("Total_Number_of_Days"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("Total_Number_of_Days"::text), '[^0-9.-]', '', 'g')::numeric
        end as total_number_of_days,
        case
            when btrim("Manager_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Manager_Approved_Time"::text)::timestamp
            when btrim("Manager_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Manager_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Manager_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}$' then to_timestamp(btrim("Manager_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI')::timestamp
            when btrim("Manager_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Manager_Approved_Time"::text), 'DD-Mon-YYYY')::timestamp
            when btrim("Manager_Approved_Time"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_timestamp(btrim("Manager_Approved_Time"::text), 'DD/MM/YYYY')::timestamp
        end as manager_approved_time,
        case
            when btrim("Director_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Director_Approved_Time"::text)::timestamp
            when btrim("Director_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Director_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Director_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}$' then to_timestamp(btrim("Director_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI')::timestamp
            when btrim("Director_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Director_Approved_Time"::text), 'DD-Mon-YYYY')::timestamp
            when btrim("Director_Approved_Time"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_timestamp(btrim("Director_Approved_Time"::text), 'DD/MM/YYYY')::timestamp
        end as director_approved_time,
        coalesce(("Donor_Budget_Line_Item"::jsonb)->>'Donor_Budget_line_Item', ("Donor_Budget_Line_Item"::jsonb)->>'zc_display_value', ("Donor_Budget_Line_Item"::jsonb)->>'Display', ("Donor_Budget_Line_Item"::jsonb)->>'Name', nullif(nullif(nullif(btrim("Donor_Budget_Line_Item"::text), ''), '[]'), '{}')) as donor_budget_line_item,
        case
            when regexp_replace(btrim("No_of_Lead_Days_for_ED"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("No_of_Lead_Days_for_ED"::text), '[^0-9.-]', '', 'g')::numeric
        end as no_of_lead_days_for_ed,
        case
            when regexp_replace(btrim("No_of_Lead_Days_for_FM"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("No_of_Lead_Days_for_FM"::text), '[^0-9.-]', '', 'g')::numeric
        end as no_of_lead_days_for_fm,
        case
            when regexp_replace(btrim("No_of_Lead_Days_for_Manager"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("No_of_Lead_Days_for_Manager"::text), '[^0-9.-]', '', 'g')::numeric
        end as no_of_lead_days_for_manager,
        case
            when regexp_replace(btrim("No_of_Lead_Days_for_Director"::text), '[^0-9.-]', '', 'g') ~ '^-?[0-9]+(\.[0-9]+)?$'
                then regexp_replace(btrim("No_of_Lead_Days_for_Director"::text), '[^0-9.-]', '', 'g')::numeric
        end as no_of_lead_days_for_director,
        case
            when btrim("Finance_Manager_Approved_Time"::text) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$' then btrim("Finance_Manager_Approved_Time"::text)::timestamp
            when btrim("Finance_Manager_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}:\d{2}$' then to_timestamp(btrim("Finance_Manager_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI:SS')::timestamp
            when btrim("Finance_Manager_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4} \d{2}:\d{2}$' then to_timestamp(btrim("Finance_Manager_Approved_Time"::text), 'DD-Mon-YYYY HH24:MI')::timestamp
            when btrim("Finance_Manager_Approved_Time"::text) ~ '^\d{2}-[A-Za-z]{3}-\d{4}$' then to_timestamp(btrim("Finance_Manager_Approved_Time"::text), 'DD-Mon-YYYY')::timestamp
            when btrim("Finance_Manager_Approved_Time"::text) ~ '^\d{2}/\d{2}/\d{4}$' then to_timestamp(btrim("Finance_Manager_Approved_Time"::text), 'DD/MM/YYYY')::timestamp
        end as finance_manager_approved_time
    from {{ source('finance_raw_data', 'All_Vendor_Advances_Finance') }} as src
)

select *
from vendor_advances
