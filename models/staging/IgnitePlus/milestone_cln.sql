with cleaned as (
    select
        coalesce(btrim("Project_ID"), '') as project_id,
        nullif(initcap(btrim("School_Name")), '') as school_name,
        nullif(initcap(btrim("Completion__")), '') as completion,
        nullif(initcap(btrim("Milestone_name")), '') as milestone_name,
        case
            when btrim("Confident_Score") ~ '^\d+(\.\d+)?$'
                then "Confident_Score"::numeric
        end as confident_score,
        nullif(initcap(btrim("Detail_of_milestone")), '') as detail_of_milestone,
        nullif(initcap(btrim("Name_of_CSR_partner")), '') as csr_partner,
        case
            when btrim("Actual_Completion_Date") <> ''
                and (
                    btrim("Actual_Completion_Date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Actual_Completion_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{2,4}$'
                )
                then to_date(
                    case
                        when btrim("Actual_Completion_Date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Actual_Completion_Date")
                        else regexp_replace(btrim("Actual_Completion_Date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Actual_Completion_Date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        when btrim("Actual_Completion_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$' then 'DD-Mon-YYYY'
                        else 'DD-Mon-YY'
                    end
                )
        end as actual_completion_date,
        nullif(initcap(btrim("Status__auto_populated_")), '') as status,
        case
            when btrim("Planned_Completion_date") <> ''
                and (
                    btrim("Planned_Completion_date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Planned_Completion_date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{2,4}$'
                )
                then to_date(
                    case
                        when btrim("Planned_Completion_date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Planned_Completion_date")
                        else regexp_replace(btrim("Planned_Completion_date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Planned_Completion_date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        when btrim("Planned_Completion_date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$' then 'DD-Mon-YYYY'
                        else 'DD-Mon-YY'
                    end
                )
        end as planned_completion_date
    from {{ source('iginteplus_25_26', 'Milestone_list') }}
)

select
    project_id,
    school_name,
    milestone_name,
    detail_of_milestone,
    csr_partner,
    actual_completion_date,
    planned_completion_date,
    status,
    completion,
    confident_score
from cleaned
where school_name is not null or project_id <> ''
