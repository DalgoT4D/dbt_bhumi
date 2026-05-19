with master as (

    select
        -- INTEGER
        case when btrim("Sl_No") ~ '^\d+$' then "Sl_No"::integer end as sl_no,
        coalesce(btrim("link_"), '') as link,
        coalesce(btrim("Remarks"), '') as remarks,

        -- TEXT
        coalesce(initcap(btrim("Quarter")), '') as quarter,
        coalesce(initcap(btrim("District")), '') as district,
        coalesce(initcap(btrim("STEM_STP")), '') as stem_stp,
        coalesce(initcap(btrim("Fellowship")), '') as fellowship,
        coalesce(initcap(btrim("Eco_Champ")), '') as eco_champ,
        coalesce(initcap(btrim("Type_of_School")), '') as school_type,
        coalesce(initcap(btrim("Financial_Year")), '') as financial_year,
        coalesce(initcap(btrim("Project_Status")), '') as project_status,
        coalesce(initcap(btrim("Name_of_CSR_Partner")), '') as csr_partner,
        coalesce(initcap(btrim("School_Classification")), '') as school_classification,
        coalesce(initcap(btrim("Name_of_school___Location")), '') as school_name,

        -- NUMERIC (FLOAT / INTEGER SAFE)
        case when btrim("School_count") ~ '^\d+(\.\d+)?$' then "School_count"::numeric end as school_count,

        case
            when btrim("Project_execution_Budget__MOU_") <> ''
                then (
                    regexp_replace(
                        split_part(btrim("Project_execution_Budget__MOU_"), E'\\t', 1),
                        '₹|,|\s',
                        '',
                        'g'
                    )::numeric
                )
        end as project_execution_budget,

        case
            when btrim("Total_project_budget__With_Branding__PM___Admin_cost___MOU_") <> ''
                then (
                    regexp_replace(
                        split_part(btrim("Total_project_budget__With_Branding__PM___Admin_cost___MOU_"), E'\\t', 1),
                        '₹|,|\s',
                        '',
                        'g'
                    )::numeric
                )
        end as total_project_budget,

        case
            when btrim("Student_count") ~ '^\d+$'
                then "Student_count"::integer
        end as student_count,
        

        -- LEVEL FIELDS (replacing with start/end dates)
        case
            when
                btrim("Planned_Start_Date") <> ''
                and (
                    btrim("Planned_Start_Date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Planned_Start_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Planned_Start_Date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Planned_Start_Date")
                        else regexp_replace(btrim("Planned_Start_Date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Planned_Start_Date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as planned_start_date,

        case
            when
                btrim("Actual_Start_date") <> ''
                and (
                    btrim("Actual_Start_date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Actual_Start_date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Actual_Start_date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Actual_Start_date")
                        else regexp_replace(btrim("Actual_Start_date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Actual_Start_date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as actual_start_date,

        case
            when
                btrim("Planned_End_date") <> ''
                and (
                    btrim("Planned_End_date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Planned_End_date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Planned_End_date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Planned_End_date")
                        else regexp_replace(btrim("Planned_End_date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Planned_End_date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as planned_end_date,

        case
            when
                btrim("Actual_End_date") <> ''
                and (
                    btrim("Actual_End_date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Actual_End_date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Actual_End_date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Actual_End_date")
                        else regexp_replace(btrim("Actual_End_date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Actual_End_date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as actual_end_date,

        -- DATE CLEANING
        case
            when
                btrim("Date_of_funds_received") <> ''
                and (
                    btrim("Date_of_funds_received") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Date_of_funds_received") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Date_of_funds_received") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Date_of_funds_received")
                        else regexp_replace(btrim("Date_of_funds_received"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Date_of_funds_received") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as funds_received_date,

        case
            when
                btrim("Date_of_Confirmation_from_CR_team") <> ''
                and (
                    btrim("Date_of_Confirmation_from_CR_team") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Date_of_Confirmation_from_CR_team") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Date_of_Confirmation_from_CR_team") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Date_of_Confirmation_from_CR_team")
                        else regexp_replace(btrim("Date_of_Confirmation_from_CR_team"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Date_of_Confirmation_from_CR_team") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as confirmation_date,

        -- PROJECT COUNTS AND DETAILS (keep as numeric if possible)
        case
            when btrim("No_of_Washroom_Constructred__Project_details_") ~ '^\d+$'
                then "No_of_Washroom_Constructred__Project_details_"::integer
        end as washroom_constructed,

        case
            when btrim("No_of_Washroom_Renovated___Project_details_") ~ '^\d+$'
                then "No_of_Washroom_Renovated___Project_details_"::integer
        end as washroom_renovated,

        case
            when btrim("No_of_Classroom_Constructred___Project_details_") ~ '^\d+$'
                then "No_of_Classroom_Constructred___Project_details_"::integer
        end as classroom_constructed,

        case
            when btrim("No_of_Classroom_Renovated___Project_details_") ~ '^\d+$'
                then "No_of_Classroom_Renovated___Project_details_"::integer
        end as classroom_renovated,

        case
            when btrim("No_of_furnitures_provided___Project_details_") ~ '^\d+$'
                then "No_of_furnitures_provided___Project_details_"::integer
        end as furniture_count,

        case
            when btrim("No_of_RO_Plant_Installed___Project_details_") ~ '^\d+$'
                then "No_of_RO_Plant_Installed___Project_details_"::integer
        end as ro_plants,

        case
            when btrim("No_of_existing_RO_Plant_AMC_extened___Project_details_") ~ '^\d+$'
                then "No_of_existing_RO_Plant_AMC_extened___Project_details_"::integer
        end as ro_plant_amc_extended,

        case
            when btrim("No_of_Solar_panel_work_Installed___Project_details_") ~ '^\d+$'
                then "No_of_Solar_panel_work_Installed___Project_details_"::integer
        end as solar_panels,

        case
            when btrim("Others___Project_details_") ~ '^\d+$'
                then "Others___Project_details_"::integer
        end as other_project_counts,

        -- TEXT REMAINING
        coalesce(initcap(btrim("Post_completion_monitored_report_Link")), '') as monitored_report_link

    from {{ source('iginteplus_25_26', 'Ignite__Master_Data_25_26') }}

),

with_delays as (

    select 
        sl_no,
        link,
        remarks,
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
        school_classification,
        school_name,
        project_execution_budget,
        total_project_budget,
        case
            when project_execution_budget <= 1000000 then 'Small'
            when project_execution_budget > 1000000 and project_execution_budget <= 5000000 then 'Medium'
            when project_execution_budget > 5000000 then 'Large'
        end as project_scale,
        student_count,
        planned_start_date,
        actual_start_date,
        planned_end_date,
        actual_end_date,
        funds_received_date,
        confirmation_date,
        case
            when planned_end_date is null or actual_end_date is null then null
            when (actual_end_date - planned_end_date) = 0 then 100.0
            when (actual_end_date - planned_end_date) >= 1 then 100.0 - (((actual_end_date - planned_end_date)::numeric / 30) * 100)
            when (actual_end_date - planned_end_date) <= -1 then 100.0 + ((actual_end_date - planned_end_date)::numeric / 30) * 100
        end as end_date_delay_percent,
        washroom_constructed,
        washroom_renovated,
        classroom_constructed,
        classroom_renovated,
        furniture_count,
        ro_plants,
        ro_plant_amc_extended,
        solar_panels,
        monitored_report_link,
        other_project_counts

    from master

)

select 
    sl_no,
    link,
    remarks,
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
    school_classification,
    school_name,
    project_execution_budget,
    total_project_budget,
    project_scale,
    student_count,
    planned_start_date,
    actual_start_date,
    planned_end_date,
    actual_end_date,
    funds_received_date,
    confirmation_date,
    end_date_delay_percent,
    washroom_constructed,
    washroom_renovated,
    classroom_constructed,
    classroom_renovated,
    furniture_count,
    ro_plants,
    ro_plant_amc_extended,
    solar_panels,
    monitored_report_link,
    other_project_counts

from with_delays
where school_name <> '' and student_count is not null
