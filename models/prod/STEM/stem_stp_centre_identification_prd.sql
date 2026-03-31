with source as (
    select
        id,
        center_name as school_name,
        donor_name as donor,
        state,
        city,
        chapter,
        program,
        project,
        center_type,
        center_status,
        overall_children_count,
        total_count,
        modified_time::date as date_modified
    from {{ ref('stem_stp_centre_identification_stg') }}
    where
        center_type = 'School'
        or center_type is null
        -- NOTE: blank center_type values are nulls from the source and are  retained. - to be checked
        or center_type = ''
)

select
    id,
    school_name,
    donor,
    state,
    city,
    chapter,
    program,
    project,
    center_type,
    center_status,

    -- academic year (April-March convention)
    case
        when date_modified is not null
            then
                (case
                    when extract(month from date_modified) >= 4
                        then extract(year from date_modified)
                    else extract(year from date_modified) - 1
                end)::integer::text
                || '-'
                || (case
                    when extract(month from date_modified) >= 4
                        then extract(year from date_modified) + 1
                    else extract(year from date_modified)
                end)::integer::text
    end as academic_year,

    -- academic quarter (Q1=Apr-Jun, Q2=Jul-Sep, Q3=Oct-Dec, Q4=Jan-Mar)
    case extract(month from date_modified)
        when 4 then 'Q1' when 5 then 'Q1' when 6 then 'Q1'
        when 7 then 'Q2' when 8 then 'Q2' when 9 then 'Q2'
        when 10 then 'Q3' when 11 then 'Q3' when 12 then 'Q3'
        when 1 then 'Q4' when 2 then 'Q4' when 3 then 'Q4'
    end as academic_quarter,

    overall_children_count,
    total_count,
    date_modified
from source
