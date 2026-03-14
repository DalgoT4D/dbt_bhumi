-- strips non-date characters before passing into validate_date
with clean_dates as (
    select
        *,
        regexp_replace("Donor"::text, '[^0-9./\-]', '', 'g') as donor_clean,
        regexp_replace("Invoice_date"::text, '[^0-9./\-]', '', 'g') as invoice_date_clean,
        regexp_replace("InvoiceReportDate"::text, '[^0-9./\-]', '', 'g') as invoice_report_date_clean,
        regexp_replace("Event_Event_End_Date"::text, '[^0-9A-Za-z./\-]', '', 'g') as event_end_date_clean,
        regexp_replace("Event_Event_Start_Date"::text, '[^0-9A-Za-z./\-]', '', 'g') as event_start_date_clean
    from {{ source('zc_bvms_data', 'Corporate_Catalyse_Tracker') }}
),

corporate as (
    select
        -- identifiers
        nullif(btrim("ID"::text), '') as id,
        nullif(btrim("Event_ID"::text), '') as event_id,

        -- event info (jsonb)
        coalesce(("Event"::jsonb)->>'zc_display_value', btrim("Event"::text), '') as event_name,
        coalesce(("Event_City"::jsonb)->>'zc_display_value', btrim("Event_City"::text), '') as event_city,
        coalesce(("Event_Address"::jsonb)->>'zc_display_value', btrim("Event_Address"::text), '') as event_address,
        coalesce(("Event_Corporate_Event_Type"::jsonb)->>'zc_display_value', btrim("Event_Corporate_Event_Type"::text), '') as corporate_event_type,
        coalesce(("Event_Corporate_Partner_Name"::jsonb)->>'zc_display_value', btrim("Event_Corporate_Partner_Name"::text), '') as corporate_partner_name,
        coalesce(("Event_Corporate_Partner_Name"::jsonb)->>'ID', btrim("Event_Corporate_Partner_Name"::text), '') as corporate_partner_id,
        coalesce(("Event_closed_by_all_aspect"::jsonb)->>'zc_display_value', btrim("Event_closed_by_all_aspect"::text), '') as event_closed_by_all_aspect,
        coalesce(("Event_CV_Event_implementer_name"::jsonb)->>'zc_display_value', btrim("Event_CV_Event_implementer_name"::text), '') as cv_event_implementer_name,

        -- coordinator (jsonb)
        coalesce(("Coordinator"::jsonb)->>'zc_display_value', btrim("Coordinator"::text), '') as coordinator_name,

        -- impact indicators (jsonb)
        coalesce(("Impact_Indicators_1"::jsonb)->>'zc_display_value', btrim("Impact_Indicators_1"::text), '') as impact_indicator_1,
        coalesce(("Impact_Indicators_2"::jsonb)->>'zc_display_value', btrim("Impact_Indicators_2"::text), '') as impact_indicator_2,

        -- event details (varchar)
        coalesce(btrim("Event_Event_Type"::text), '') as event_type,
        coalesce(btrim("Event_Event_End_Time"::text), '') as event_end_time,
        coalesce(btrim("Event_Event_Start_Time"::text), '') as event_start_time,
        coalesce(btrim("Event_Type_of_implementation"::text), '') as type_of_implementation,
        coalesce(btrim("Event_posted_on_social_media"::text), '') as posted_on_social_media,
        coalesce(btrim("Event_Client_Coordinator_Name"::text), '') as client_coordinator_name,
        coalesce(btrim("Event_Beneficiary_Details"::text), '') as beneficiary_details,
        coalesce(btrim("Event_Beneficiary_Location"::text), '') as beneficiary_location,

        -- dates
        {{ validate_date('donor_clean') }} as donor_date,
        {{ validate_date('invoice_date_clean') }} as invoice_date,
        {{ validate_date('invoice_report_date_clean') }} as invoice_report_date,
        {{ validate_date('event_end_date_clean') }} as event_end_date,
        {{ validate_date('event_start_date_clean') }} as event_start_date,

        -- financials (float)
        case when btrim("Balance"::text) ~ '^[0-9.]+$' then ("Balance"::text)::numeric end as balance,
        case when btrim("Total_Spent"::text) ~ '^[0-9.]+$' then ("Total_Spent"::text)::numeric end as total_spent,
        case when btrim("Actual_Budget"::text) ~ '^[0-9.]+$' then ("Actual_Budget"::text)::numeric end as actual_budget,
        case when btrim("Invoice_Value"::text) ~ '^[0-9.]+$' then ("Invoice_Value"::text)::numeric end as invoice_value,
        case when btrim("Event_Planned_Budget"::text) ~ '^[0-9.]+$' then ("Event_Planned_Budget"::text)::numeric end as event_planned_budget,
        case when btrim("Area_of_walls_painted_in_Sq_Ft"::text) ~ '^[0-9.]+$' then ("Area_of_walls_painted_in_Sq_Ft"::text)::numeric end as area_of_walls_painted_sqft,
        case when btrim("Amount_of_dry_waste_collected_in_Kgs"::text) ~ '^[0-9.]+$' then ("Amount_of_dry_waste_collected_in_Kgs"::text)::numeric end as dry_waste_collected_kgs,
        case when btrim("Expense_reported_via_Advance_on_Happay"::text) ~ '^[0-9.]+$' then ("Expense_reported_via_Advance_on_Happay"::text)::numeric end as expense_via_advance_happay,
        case when btrim("Expense_reported_via_Invoicing_on_Happay"::text) ~ '^[0-9.]+$' then ("Expense_reported_via_Invoicing_on_Happay"::text)::numeric end as expense_via_invoicing_happay,
        case when btrim("Event_Budget_sanctioned_by_the_Corporate_on_Email"::text) ~ '^[0-9.]+$' then ("Event_Budget_sanctioned_by_the_Corporate_on_Email"::text)::numeric end as budget_sanctioned_by_corporate,

        -- counts (int)
        case when btrim("Key_result_1"::text) ~ '^[0-9]+$' then ("Key_result_1"::text)::integer end as key_result_1,
        case when btrim("Key_result_2"::text) ~ '^[0-9]+$' then ("Key_result_2"::text)::integer end as key_result_2,
        case when btrim("No_of_DIY_kit"::text) ~ '^[0-9]+$' then ("No_of_DIY_kit"::text)::integer end as no_of_diy_kit,
        case when btrim("Number_Of_CSAT"::text) ~ '^[0-9]+$' then ("Number_Of_CSAT"::text)::integer end as number_of_csat,
        case when btrim("No_of_learning_aid"::text) ~ '^[0-9]+$' then ("No_of_learning_aid"::text)::integer end as no_of_learning_aid,
        case when btrim("No_of_Beneficiaries"::text) ~ '^[0-9]+$' then ("No_of_Beneficiaries"::text)::integer end as no_of_beneficiaries,
        case when btrim("Acual_number_of_volunteers"::text) ~ '^[0-9]+$' then ("Acual_number_of_volunteers"::text)::integer end as actual_no_of_volunteers,
        case when btrim("No_of_lives_touched_in_Nos"::text) ~ '^[0-9]+$' then ("No_of_lives_touched_in_Nos"::text)::integer end as no_of_lives_touched,
        case when btrim("No_of_DIY_kit_assembled_in_Nos"::text) ~ '^[0-9]+$' then ("No_of_DIY_kit_assembled_in_Nos"::text)::integer end as no_of_diy_kit_assembled,
        case when btrim("No_of_saplings_planted_in_Units"::text) ~ '^[0-9]+$' then ("No_of_saplings_planted_in_Units"::text)::integer end as no_of_saplings_planted,
        case when btrim("Total_volunteering_hours_engaged"::text) ~ '^[0-9.]+$' then ("Total_volunteering_hours_engaged"::text)::numeric end as total_volunteering_hours,
        case when btrim("Number_of_volunteers_participated"::text) ~ '^[0-9]+$' then ("Number_of_volunteers_participated"::text)::integer end as no_of_volunteers_participated,
        case when btrim("Event_Expected_number_of_volunteers"::text) ~ '^[0-9]+$' then ("Event_Expected_number_of_volunteers"::text)::integer end as expected_no_of_volunteers,
        case when btrim("Event_Total_No_of_Beneficiary"::text) ~ '^[0-9]+$' then ("Event_Total_No_of_Beneficiary"::text)::integer end as total_no_of_beneficiary,

        -- other varchar fields
        coalesce(btrim("CSAT"::text), '') as csat,
        coalesce(btrim("Remarks"::text), '') as remarks,
        coalesce(btrim("Testimonials"::text), '') as testimonials,
        coalesce(btrim("Invoice_number"::text), '') as invoice_number,
        coalesce(btrim("Happay_Report_ID"::text), '') as happay_report_id,
        coalesce(btrim("Happay_Invoice_ID"::text), '') as happay_invoice_id,
        coalesce(btrim("Happay_reference_number"::text), '') as happay_reference_number,
        coalesce(btrim("Brief_summary_of_event"::text), '') as brief_summary,
        coalesce(btrim("Coordinator_Added_Time"::text), '') as coordinator_added_time,
        coalesce(btrim("Duration_of_volunteers_engaged"::text), '') as duration_volunteers_engaged,
        coalesce(btrim("Duration_of_activity_in_minutes"::text), '') as duration_of_activity_mins,
        coalesce(btrim("Planned_Budget_Total_Actual_Expense_Split_up"::text), '') as planned_budget_actual_expense_splitup,
        coalesce(btrim("Can_we_use_the_corporate_s_name_and_logo_in_social"::text), '') as use_corporate_name_logo_in_social

    from clean_dates
)

select distinct
    id,
    event_id,
    event_name,
    event_city,
    event_address,
    corporate_event_type,
    corporate_partner_name,
    corporate_partner_id,
    event_closed_by_all_aspect,
    cv_event_implementer_name,
    coordinator_name,
    impact_indicator_1,
    impact_indicator_2,
    event_type,
    event_end_time,
    event_start_time,
    type_of_implementation,
    posted_on_social_media,
    client_coordinator_name,
    beneficiary_details,
    beneficiary_location,
    donor_date,
    invoice_date,
    invoice_report_date,
    event_end_date,
    event_start_date,
    balance,
    total_spent,
    actual_budget,
    invoice_value,
    event_planned_budget,
    area_of_walls_painted_sqft,
    dry_waste_collected_kgs,
    expense_via_advance_happay,
    expense_via_invoicing_happay,
    budget_sanctioned_by_corporate,
    key_result_1,
    key_result_2,
    no_of_diy_kit,
    number_of_csat,
    no_of_learning_aid,
    no_of_beneficiaries,
    actual_no_of_volunteers,
    no_of_lives_touched,
    no_of_diy_kit_assembled,
    no_of_saplings_planted,
    total_volunteering_hours,
    no_of_volunteers_participated,
    expected_no_of_volunteers,
    total_no_of_beneficiary,
    csat,
    remarks,
    testimonials,
    invoice_number,
    happay_report_id,
    happay_invoice_id,
    happay_reference_number,
    brief_summary,
    coordinator_added_time,
    duration_volunteers_engaged,
    duration_of_activity_mins,
    planned_budget_actual_expense_splitup,
    use_corporate_name_logo_in_social
from corporate
