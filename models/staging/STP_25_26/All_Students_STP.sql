-- Clean and format STP 25-26 All_Students data
-- Converts raw data types and cleans values

select
    -- Identifiers
    coalesce(nullif(trim("id"::text), ''), null)::bigint as "ID",
    coalesce(nullif(trim("student_adhar_card_number"::text), ''), null) as "Aadhar Number",
    coalesce(nullif(trim("student_school_roll_no_"::text), ''), null) as "Roll Number",
    
    -- Basic Info
    coalesce(initcap(trim("name"::text)), '') as "Student Name",
    coalesce(trim("gender"::text), '') as Gender,
    case 
        when trim("academic_year"::text) ~ '^\d{4}-\d{4}$' then trim("academic_year"::text)
        else null
    end as "Academic Year",
    
    -- School Info
    coalesce(initcap(trim("school_name"::text)), '') as "School Name",
    coalesce(trim("class"::text), '') as Class,
    coalesce(trim("class_division"::text), '') as "Class Division",
    
    -- Contact Info
    coalesce(
        case 
            when trim("contact_number"::text) ~ '^\d{10}$' then trim("contact_number"::text)
            when trim("contact_number"::text) ~ '^\+91\d{10}$' then substring(trim("contact_number"::text), 4)
            else null
        end,
        ''
    ) as "Contact Number",
    
    -- Parents Info
    coalesce(initcap(trim("father_name"::text)), '') as "Father Name",
    coalesce(
        case 
            when trim("father_phone"::text) ~ '^\d{10}$' then trim("father_phone"::text)
            when trim("father_phone"::text) ~ '^\+91\d{10}$' then substring(trim("father_phone"::text), 4)
            else null
        end,
        ''
    ) as "Father Phone",
    coalesce(trim("father_email"::text), '') as "Father Email",
    coalesce(
        case 
            when trim("father_income"::text) ~ '^\d+(\.\d{1,2})?$' then (trim("father_income"::text))::numeric
            else null
        end
    ) as "Father Income",
    
    coalesce(initcap(trim("mother_name"::text)), '') as "Mother Name",
    coalesce(
        case 
            when trim("mother_phone"::text) ~ '^\d{10}$' then trim("mother_phone"::text)
            when trim("mother_phone"::text) ~ '^\+91\d{10}$' then substring(trim("mother_phone"::text), 4)
            else null
        end,
        ''
    ) as "Mother Phone",
    coalesce(trim("mother_email"::text), '') as "Mother Email",
    coalesce(
        case 
            when trim("mother_income"::text) ~ '^\d+(\.\d{1,2})?$' then (trim("mother_income"::text))::numeric
            else null
        end
    ) as "Mother Income",
    
    -- Status & Admin
    coalesce(trim("status"::text), '') as Status,
    coalesce(initcap(trim("donor"::text)), '') as Donor,
    coalesce(trim("project_name"::text), '') as "Project Name",
    
    -- Location Info
    coalesce(trim("state"::text), '') as State,
    coalesce(trim("district"::text), '') as District,
    
    -- Staff Info
    coalesce(initcap(trim("trainer_name"::text)), '') as "Trainer Name",
    coalesce(initcap(trim("reporting_manager"::text)), '') as "Reporting Manager",
    coalesce(initcap(trim("zonal_manager"::text)), '') as "Zonal Manager",
    
    -- Dates
    case 
        when trim("date_of_joining_the_school"::text) ~ '^\d{4}-\d{2}-\d{2}$' 
            then (trim("date_of_joining_the_school"::text))::date
        when trim("date_of_joining_the_school"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            then to_date(trim("date_of_joining_the_school"::text), 'DD/MM/YYYY')
        when trim("date_of_joining_the_school"::text) ~ '^\d{1,2}/\d{1,2}/\d{2}$'
            then to_date(trim("date_of_joining_the_school"::text), 'DD/MM/YY')
        else null
    end as "Date of Joining the School",
    
    case 
        when trim("added_time"::text) ~ '^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}$'
            then (trim("added_time"::text))::timestamp
        else null
    end as "Added Time"

from {{ source('STP_25-26', 'All_Students') }}
where ID is not null
order by "Roll Number"
