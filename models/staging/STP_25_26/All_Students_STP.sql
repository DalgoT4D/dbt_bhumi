-- Clean and format STP 25-26 All_Students data
-- Converts raw data types and cleans values

select
    -- Identifiers
    coalesce(nullif(trim("ID"::text), ''), null)::bigint as student_id,
    coalesce(nullif(trim("Student Adhar Card Number"::text), ''), null) as aadhar_number,
    coalesce(nullif(trim("Student School Roll no."::text), ''), null) as roll_number,
    
    -- Basic Info
    coalesce(initcap(trim("Name"::text)), '') as student_name,
    coalesce(trim("Gender"::text), '') as gender,
    case 
        when trim("Academic Year"::text) ~ '^\d{4}-\d{4}$' then trim("Academic Year"::text)
        else null
    end as academic_year,
    
    -- School Info
    coalesce(initcap(trim("School Name"::text)), '') as school_name,
    coalesce(trim("Class"::text), '') as class,
    coalesce(trim("Class Division"::text), '') as class_division,
    
    -- Contact Info
    coalesce(
        case 
            when trim("Contact Number"::text) ~ '^\d{10}$' then trim("Contact Number"::text)
            when trim("Contact Number"::text) ~ '^\+91\d{10}$' then substring(trim("Contact Number"::text), 4)
            else null
        end,
        ''
    ) as contact_number,
    
    -- Parents Info
    coalesce(initcap(trim("Father Name"::text)), '') as father_name,
    coalesce(
        case 
            when trim("Father Phone"::text) ~ '^\d{10}$' then trim("Father Phone"::text)
            when trim("Father Phone"::text) ~ '^\+91\d{10}$' then substring(trim("Father Phone"::text), 4)
            else null
        end,
        ''
    ) as father_phone,
    coalesce(trim("Father Email"::text), '') as father_email,
    coalesce(
        case 
            when trim("Father Income"::text) ~ '^\d+(\.\d{1,2})?$' then (trim("Father Income"::text))::numeric
            else null
        end
    ) as father_income,
    
    coalesce(initcap(trim("Mother Name"::text)), '') as mother_name,
    coalesce(
        case 
            when trim("Mother Phone"::text) ~ '^\d{10}$' then trim("Mother Phone"::text)
            when trim("Mother Phone"::text) ~ '^\+91\d{10}$' then substring(trim("Mother Phone"::text), 4)
            else null
        end,
        ''
    ) as mother_phone,
    coalesce(trim("Mother Email"::text), '') as mother_email,
    coalesce(
        case 
            when trim("Mother Income"::text) ~ '^\d+(\.\d{1,2})?$' then (trim("Mother Income"::text))::numeric
            else null
        end
    ) as mother_income,
    
    -- Status & Admin
    coalesce(trim("Status"::text), '') as student_status,
    coalesce(initcap(trim("Donor"::text)), '') as donor,
    coalesce(trim("Project Name"::text), '') as project_name,
    
    -- Location Info
    coalesce(trim("State"::text), '') as state,
    coalesce(trim("District"::text), '') as district,
    
    -- Staff Info
    coalesce(initcap(trim("Trainer Name"::text)), '') as trainer_name,
    coalesce(initcap(trim("Reporting Manager"::text)), '') as reporting_manager,
    coalesce(initcap(trim("Zonal Manager"::text)), '') as zonal_manager,
    
    -- Dates
    case 
        when trim("Date of Joining the School"::text) ~ '^\d{4}-\d{2}-\d{2}$' 
            then (trim("Date of Joining the School"::text))::date
        when trim("Date of Joining the School"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            then to_date(trim("Date of Joining the School"::text), 'DD/MM/YYYY')
        when trim("Date of Joining the School"::text) ~ '^\d{1,2}/\d{1,2}/\d{2}$'
            then to_date(trim("Date of Joining the School"::text), 'DD/MM/YY')
        else null
    end as date_of_joining,
    
    case 
        when trim("Added Time"::text) ~ '^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}$'
            then (trim("Added Time"::text))::timestamp
        else null
    end as record_added_time

from {{ source('STP_25-26', 'All_Students') }}
where "ID" is not null
order by student_id
