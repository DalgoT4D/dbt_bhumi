{{ config(
    materialized='table',
    tags=["civic_clubs"]
) }}

with stg as (
    select * from {{ ref('cc_overallclubs_stg') }}
),

with_month_num as (
    select
        *,
        extract(month from to_date(month, 'Month'))::int as month_num
    from stg
),

with_calculated_fields as (
    select
        year,
        month,
        case
            when month_num >= 4
                then lpad(((year::int % 100))::text, 2, '0')
                    || '-'
                    || lpad((((year::int + 1) % 100))::text, 2, '0')
            else
                lpad((((year::int - 1) % 100))::text, 2, '0')
                    || '-'
                    || lpad(((year::int % 100))::text, 2, '0')
        end as academic_year,
        club_name,
        city,
        region,
        leaders,
        edu_engaged,
        orientation,
        civic_engaged,
        classes_taken,
        shelters_mapped,
        total_edu_vols,
        civic_activities,
        total_civic_vols,
        case
            when (classes_taken + civic_activities + orientation) > 0 then 'Active'
            else 'Inactive'
        end as club_status
    from with_month_num
)

select * from with_calculated_fields
