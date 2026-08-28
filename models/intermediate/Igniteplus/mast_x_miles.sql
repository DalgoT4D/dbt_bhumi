{{ config(
  materialized='table',
  tags=["igniteplus", "int"]
) }}

select
    mast.project_id,
    mast.financial_year,
    mast.quarter,
    mast.school_name,
    mast.district,
    mast.school_type,
    mast.school_classification,
    mast.csr_partner,
    mast.project_status,
    mast.project_scale,
    mast.project_execution_budget,
    mast.total_project_budget,
    mast.link as project_link,
    milestone.milestone,
    milestone.milestone_name,
    milestone.completion_perc as milestone_completion_perc

from {{ ref('master_data_ign_25_26') }} as mast
left join {{ ref('milestone_cln') }} as milestone
    on mast.project_id = milestone.project_id
