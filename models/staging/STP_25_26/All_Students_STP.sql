-- Clean and format STP 25-26 All_Students data
-- Converts raw data types and cleans values

select
    -- Identifiers
    coalesce(nullif(trim("ID"::text), ''), null)::bigint as "Student ID",
    coalesce(nullif(trim("Student_Adhar_Card_Number"::text), ''), null) as "Aadhar Number",
    coalesce(nullif(trim("Student_School_Roll_no_"::text), ''), null) as "Roll Number",
    
    -- Basic Info
    coalesce(initcap(trim("Name"::text)), '') as "Student Name",
    coalesce(trim("Gender"::text), '') as Gender,
    case 
        when trim("Academic_Year"::text) ~ '^\d{4}-\d{4}$' then trim("Academic_Year"::text)
        else null
    end as "Academic Year",
    
    -- School Info
    coalesce(initcap(trim("School_Name"::text)), '') as "School Name",
    coalesce(trim("Class"::text), '') as Class,
    coalesce(trim("Class_Division"::text), '') as "Class Division",
    
    -- Contact Info
    coalesce(
        case 
            when trim("Contact_Number"::text) ~ '^\d{10}$' then trim("Contact_Number"::text)
            when trim("Contact_Number"::text) ~ '^\+91\d{10}$' then substring(trim("Contact_Number"::text), 4)
            else null
        end,
        ''
    ) as "Contact Number",
    
    -- Parents Info
    coalesce(initcap(trim("Father Name"::text)), '') as "Father Name",
    coalesce(
        case 
            when trim("Father_Phone"::text) ~ '^\d{10}$' then trim("Father_Phone"::text)
            when trim("Father_Phone"::text) ~ '^\+91\d{10}$' then substring(trim("Father_Phone"::text), 4)
            else null
        end,
        ''
    ) as "Father Phone",
    coalesce(trim("Father_Email"::text), '') as "Father Email",
    coalesce(
        case 
            when trim("Father_Income"::text) ~ '^\d+(\.\d{1,2})?$' then (trim("Father_Income"::text))::numeric
            else null
        end
    ) as "Father Income",
    
    coalesce(initcap(trim("Mother_Name"::text)), '') as "Mother Name",
    coalesce(
        case 
            when trim("Mother_Phone"::text) ~ '^\d{10}$' then trim("Mother_Phone"::text)
            when trim("Mother_Phone"::text) ~ '^\+91\d{10}$' then substring(trim("Mother_Phone"::text), 4)
            else null
        end,
        ''
    ) as "Mother Phone",
    coalesce(trim("Mother_Email"::text), '') as "Mother Email",
    coalesce(
        case 
            when trim("Mother_Income"::text) ~ '^\d+(\.\d{1,2})?$' then (trim("Mother_Income"::text))::numeric
            else null
        end
    ) as "Mother Income",
    
    -- Status & Admin
    coalesce(trim("Status"::text), '') as "Status",
    coalesce(initcap(trim("Donor"::text)), '') as Donor,
    coalesce(trim("Project_Name"::text), '') as "Project Name",
    
    -- Location Info
    coalesce(trim("State"::text), '') as "State",
    coalesce(trim("District"::text), '') as District,
    
    -- Staff Info
    coalesce(initcap(trim("Trainer_Name"::text)), '') as "Trainer Name",
    coalesce(initcap(trim("Reporting_Manager"::text)), '') as "Reporting Manager",
    coalesce(initcap(trim("Zonal_Manager"::text)), '') as "Zonal Manager",
    
    -- Dates
    case 
        when trim("Date_of_Joining_the_School"::text) ~ '^\d{4}-\d{2}-\d{2}$' 
            then (trim("Date_of_Joining_the_School"::text))::date
        when trim("Date_of_Joining_the_School"::text) ~ '^\d{1,2}/\d{1,2}/\d{4}$'
            then to_date(trim("Date_of_Joining_the_School"::text), 'DD/MM/YYYY')
        when trim("Date_of_Joining_the_School"::text) ~ '^\d{1,2}/\d{1,2}/\d{2}$'
            then to_date(trim("Date_of_Joining_the_School"::text), 'DD/MM/YY')
        else null
    end as "Date of Joining the School",
    
    case 
        when trim("Added_Time"::text) ~ '^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}$'
            then (trim("Added_Time"::text))::timestamp
        else null
    end as "Added Time"

from {{ source('STP_25-26', 'All_Students') }}
where "ID" is not null
order by student_id
