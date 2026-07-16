with master_metrics as (
    select
        project_id,
        financial_year,
        quarter,
        district,
        school_type,
        project_status,
        school_count,
        csr_partner,
        school_name,
        project_execution_budget,
        total_project_budget,
        project_scale,
        student_count,
        washroom_constructed,
        washroom_renovated,
        classroom_constructed,
        classroom_renovated,
        furniture_count,
        ro_plants,
        ro_plant_amc_extended,
        solar_panels,
        other_project_counts
    from {{ ref('master_data_ign_25_26') }}
    where project_id <> ''
),

milestone_rows as (
    select
        project_id,
        school_name,
        csr_partner,
        milestone_name,
        status,
        completion,
        confident_score,
        row_number() over (
            partition by project_id
            order by
                coalesce(
                    nullif(regexp_replace(trim(milestone_name::text), '[^0-9]', '', 'g'), '')::integer,
                    0
                ) desc,
                milestone_name desc
        ) as rn
    from {{ ref('milestone_cln') }}
    where project_id <> ''
),

milestone_last as (
    select
        project_id,
        school_name,
        csr_partner,
        status as raw_status
    from milestone_rows
    where rn = 1
),

milestone_agg as (
    select
        project_id,
        avg(nullif(regexp_replace(completion::text, '[^0-9.]', '', 'g'), '')::numeric) as milestone_completion,
        avg(nullif(regexp_replace(confident_score::text, '[^0-9.]', '', 'g'), '')::numeric) as milestone_confident_score
    from {{ ref('milestone_cln') }}
    where project_id <> ''
    group by project_id
),

milestone_projects as (
    select
        coalesce(l.project_id, a.project_id) as project_id,
        l.school_name,
        l.csr_partner,
        case
            when l.raw_status is null or l.raw_status = '' then 'Ongoing'
            when lower(l.raw_status) = 'completed' then 'On track'
            when lower(l.raw_status) in ('delayed','delay') or lower(l.raw_status) like '%delay%' then 'Off track'
            else initcap(l.raw_status)
        end as status,
        a.milestone_completion,
        a.milestone_confident_score
    from milestone_last as l
    full join milestone_agg as a on l.project_id = a.project_id
)

select
    m.project_id,
    m.financial_year,
    m.quarter,
    m.district,
    m.school_type,
    m.project_status,
    m.school_count,
    m.csr_partner,
    m.school_name,
    m.project_execution_budget,
    m.total_project_budget,
    m.project_scale,
    m.student_count,
    m.washroom_constructed,
    m.washroom_renovated,
    m.classroom_constructed,
    m.classroom_renovated,
    m.furniture_count,
    m.ro_plants,
    m.ro_plant_amc_extended,
    m.solar_panels,
    mp.status as milestone_status,
    mp.milestone_completion,
    mp.milestone_confident_score
from master_metrics as m
left join milestone_projects as mp
    on m.project_id = mp.project_id
