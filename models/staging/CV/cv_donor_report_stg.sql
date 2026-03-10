-- strips non-date characters while preserving month abbreviation letters (dd-mon-yyyy)
with clean_dates as (
    select
        *,
        regexp_replace("Invoice_Date"::text, '[^0-9A-Za-z./\-]', '', 'g') as invoice_date_clean
    from {{ source('zc_bvms_data', 'Donor_Report_Report') }}
),

donor_report as (
    select
        nullif(btrim("ID"::text), '') as id,

        -- event (jsonb)
        nullif(btrim(("Event"::jsonb)->>'ID'), '') as event_id,
        coalesce(("Event"::jsonb)->>'zc_display_value', btrim("Event"::text), '') as event_name,

        -- date
        {{ validate_date('invoice_date_clean') }} as invoice_date,

        -- POC details
        coalesce(initcap(btrim("Name_to_the_POC"::text)), '') as poc_name,
        lower(nullif(btrim("Email_of_the_POC"::text), '')) as poc_email

    from clean_dates
)

select distinct
    id,
    event_id,
    event_name,
    invoice_date,
    poc_name,
    poc_email
from donor_report
