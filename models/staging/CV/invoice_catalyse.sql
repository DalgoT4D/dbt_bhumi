{{ config(
  materialized='table',
  tags=['cv', 'staging']
) }}

with source as (
    select
        "ID" as id,
        "Event" as event_raw,
        "Event_City" as event_city_raw,
        "Coordinator" as coordinator_raw,
        "Upload_Invoice" as upload_invoice,
        "InvoiceReportDate" as invoice_report_date_raw,
        "Event_Corporate_Event_Type" as corporate_event_type_raw,
        "Event_Corporate_Partner_Name" as corporate_partner_name_raw
    from {{ source('zc_bvms_data', 'Invoice_Corporate_Catalyse_Tracker') }}
),

normalized as (
    select
        nullif(btrim(id::text), '') as id,
        nullif(btrim((event_raw::jsonb)->>'ID'), '') as event_id,
        coalesce(
            nullif(btrim((event_raw::jsonb)->>'Event_Name'), ''),
            nullif(btrim((event_raw::jsonb)->>'Event_name'), ''),
            nullif(btrim((event_raw::jsonb)->>'zc_display_value'), ''),
            nullif(btrim(event_raw::text), '')
        ) as event_name,
        coalesce(
            nullif(btrim((event_raw::jsonb)->>'Event_Start_Date'), ''),
            nullif(btrim((event_raw::jsonb)->>'Event_Date'), ''),
            nullif(btrim((event_raw::jsonb)->>'Start_Date'), ''),
            nullif(btrim((event_raw::jsonb)->>'event_date'), '')
        ) as event_date_raw,
        coalesce(
            nullif(btrim((event_city_raw::jsonb)->>'City'), ''),
            nullif(btrim((event_city_raw::jsonb)->>'city'), ''),
            nullif(btrim((event_city_raw::jsonb)->>'zc_display_value'), ''),
            nullif(btrim(event_city_raw::text), '')
        ) as event_city,
        coalesce(
            nullif(btrim((coordinator_raw::jsonb)->>'Name'), ''),
            nullif(btrim((coordinator_raw::jsonb)->>'name'), ''),
            nullif(btrim((coordinator_raw::jsonb)->>'zc_display_value'), ''),
            nullif(btrim(coordinator_raw::text), '')
        ) as coordinator_name,
        nullif(btrim(upload_invoice::text), '') as upload_invoice,
        nullif(btrim(invoice_report_date_raw::text), '') as invoice_report_date_raw,
        coalesce(
            nullif(btrim((corporate_event_type_raw::jsonb)->>'Name'), ''),
            nullif(btrim((corporate_event_type_raw::jsonb)->>'name'), ''),
            nullif(btrim((corporate_event_type_raw::jsonb)->>'zc_display_value'), ''),
            nullif(btrim(corporate_event_type_raw::text), '')
        ) as corporate_event_type,
        coalesce(
            nullif(btrim((corporate_partner_name_raw::jsonb)->>'Name'), ''),
            nullif(btrim((corporate_partner_name_raw::jsonb)->>'name'), ''),
            nullif(btrim((corporate_partner_name_raw::jsonb)->>'zc_display_value'), ''),
            nullif(btrim(corporate_partner_name_raw::text), '')
        ) as corporate_partner_name
    from source
),

final as (
    select
        id,
        event_id,
        event_name,
        {{ validate_date('event_date_raw') }} as event_date,
        event_city,
        coordinator_name,
        upload_invoice,
        {{ validate_date('invoice_report_date_raw') }} as invoice_report_date,
        corporate_event_type,
        corporate_partner_name
    from normalized
),

tat_days as (
    select
        *,
        case
            when event_date is not null and invoice_report_date is not null
                then
                    (invoice_report_date - event_date)
        end as invoice_tat_days
    from final
),

is_sla_met as (
    select
        *,
        ((invoice_tat_days <= 5)) as is_invoice_sla_met
    from tat_days
)

select *
from is_sla_met
