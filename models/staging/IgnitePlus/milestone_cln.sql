with cleaned as (
    select
        coalesce(btrim("Project_ID"), '') as project_id,
        coalesce(initcap(btrim("School_Name")), '') as school_name,
        case
            when btrim("Milestone_name") ~ '\d+'
                then (regexp_match(btrim("Milestone_name"), '\d+'))[1]::integer
        end as milestone,

        coalesce(initcap(btrim("Detail_of_milestone")), '') as milestone_name,
        coalesce(initcap(btrim("Name_of_CSR_partner")), '') as csr_partner,

        case
            when btrim("Actual_Completion_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                then to_date(
                    regexp_replace(btrim("Actual_Completion_Date"), '\s*-\s*', '-', 'g'),
                    'DD-Mon-YYYY'
                )
        end as actual_completion_date,

        case
            when btrim("Planned_Completion_date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                then to_date(
                    regexp_replace(btrim("Planned_Completion_date"), '\s*-\s*', '-', 'g'),
                    'DD-Mon-YYYY'
                )
        end as planned_completion_date,

        coalesce(initcap(btrim("Status")), '') as status,
        case
            when
                btrim("Completion__") <> ''
                and btrim("Completion__") ~ '^\d+(\.\d+)?$'
                then "Completion__"::numeric
        end as completion_perc
        
    from {{ source('iginteplus_25_26', 'Milestone_list') }}
)

select
    project_id,
    school_name,
    milestone,
    milestone_name,
    csr_partner,
    actual_completion_date,
    planned_completion_date,
    status,
    completion_perc
from cleaned
where school_name is not null or project_id <> ''
