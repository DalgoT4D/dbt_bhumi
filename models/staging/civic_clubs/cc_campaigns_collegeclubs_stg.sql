{{ config(
  materialized='table',
  tags=["civic_clubs"]
) }}

with cleaned as (
    select
        case
            when "Quarter" like 'Q1%' then 'Q1'
            when "Quarter" like 'Q2%' then 'Q2'
            when "Quarter" like 'Q3%' then 'Q3'
            when "Quarter" like 'Q4%' then 'Q4'
            else "Quarter"
        end as quarter,
        trim("Parameters") as parameter,
        cast(replace("Achieved", ',', '') as int) as achieved,
        "Campaign_Name_" as campaign_name
    from {{ source('civic_clubs', 'Campaigns_Data_25_26') }}
),

pivoted as (
    select
        quarter,
        campaign_name,
        max(case when parameter = 'Num of Event Coordinators'  then achieved end) as num_event_coordinators,
        max(case when parameter = 'Num of Volunteering Hours'  then achieved end) as num_volunteering_hours,
        max(case when parameter = 'Num of Colleges Engaged'    then achieved end) as num_colleges_engaged,
        max(case when parameter = 'Num of Volunteers Engaged'  then achieved end) as num_volunteers_engaged,
        max(case when parameter = 'Num of Events Organized'    then achieved end) as num_events_organized
    from cleaned
    group by quarter, campaign_name
)

select * from pivoted
order by quarter, campaign_name
