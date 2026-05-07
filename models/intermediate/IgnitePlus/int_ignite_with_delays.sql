{{
    config(
        materialized = 'table'
    )
}}

with base as (

    select * from {{ ref('master_data_ign_25_26') }}

),

with_derived as (

    select
        *,

        -- PROJECT SCALE derived from already-cleaned total_project_budget
        case
            when total_project_budget is null then null
            when total_project_budget < 10   then 'Small'
            when total_project_budget <= 50  then 'Medium'
            else                                  'Large'
        end as project_scale,

        -- MILESTONE DELAY PERCENTS
        {{ milestone_delay_percent('m1_planned', 'm1_actual') }} as m1_delay_percent,
        {{ milestone_delay_percent('m2_planned', 'm2_actual') }} as m2_delay_percent,
        {{ milestone_delay_percent('m3_planned', 'm3_actual') }} as m3_delay_percent,
        {{ milestone_delay_percent('m4_planned', 'm4_actual') }} as m4_delay_percent,
        {{ milestone_delay_percent('m5_planned', 'm5_actual') }} as m5_delay_percent

    from base

)

select
    sl_no,
    financial_year,
    quarter,
    district,
    stem_stp,
    fellowship,
    eco_champ,
    school_type,
    project_status,
    school_count,
    csr_partner,
    school_name,
    project_execution_budget,
    total_project_budget,
    project_scale,
    student_count,
    starting_level_wash,
    current_level_wash,
    starting_level_classroom,
    current_level_classroom,
    funds_received_date,
    confirmation_date,

    m1_planned, m1_actual, m1_delay_percent,
    m2_planned, m2_actual, m2_delay_percent,
    m3_planned, m3_actual, m3_delay_percent,
    m4_planned, m4_actual, m4_delay_percent,
    m5_planned, m5_actual, m5_delay_percent,

    -- AVERAGE DELAY: only over milestones that have both planned + actual
    (
        coalesce(m1_delay_percent, 0)
        + coalesce(m2_delay_percent, 0)
        + coalesce(m3_delay_percent, 0)
        + coalesce(m4_delay_percent, 0)
        + coalesce(m5_delay_percent, 0)
    ) / nullif(
        (case when m1_delay_percent is not null then 1 else 0 end)
        + (case when m2_delay_percent is not null then 1 else 0 end)
        + (case when m3_delay_percent is not null then 1 else 0 end)
        + (case when m4_delay_percent is not null then 1 else 0 end)
        + (case when m5_delay_percent is not null then 1 else 0 end),
        0
    )                                                               as avg_delay_percent,

    washroom_constructed,
    washroom_renovated,
    classroom_constructed,
    classroom_renovated,
    furniture_count,
    ro_plants,
    solar_panels,
    other_project_counts,
    monitored_report_link

from with_derived
where school_name <> ''
  and student_count is not null
