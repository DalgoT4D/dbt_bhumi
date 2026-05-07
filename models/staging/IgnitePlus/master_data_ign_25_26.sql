with master as (

    select
        -- INTEGER
        case when btrim("Sl_No") ~ '^\d+$' then "Sl_No"::integer end as sl_no,
        -- coalesce(initcap(btrim("Remarks")), '') as remarks,
        -- coalesce(btrim("Folder_link_"), '') as folder_link,

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
            when btrim("Actual_Considerable_Student_count__As_per_standard_norms___thro") ~ '^\d+$'
                then "Actual_Considerable_Student_count__As_per_standard_norms___thro"::integer
        end as student_count,

        -- LEVELS (text fields)
        coalesce(initcap(btrim("Starting_Level___WASH")), '') as starting_level_wash,
        coalesce(initcap(btrim("Current_Level___WASH")), '') as current_level_wash,
        coalesce(initcap(btrim("Starting_Level___Classroom")), '') as starting_level_classroom,
        coalesce(initcap(btrim("Current_Level___Classroom")), '') as current_level_classroom,

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

        -- MILESTONE DATES
        case
            when
                btrim("Milestone_1_Planned_Date") <> ''
                and (
                    btrim("Milestone_1_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Milestone_1_Planned_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Milestone_1_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Milestone_1_Planned_Date")
                        else regexp_replace(btrim("Milestone_1_Planned_Date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Milestone_1_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as m1_planned,
        case
            when
                btrim("Milestone_1_Actual_Date") <> ''
                and (
                    btrim("Milestone_1_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Milestone_1_Actual_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Milestone_1_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Milestone_1_Actual_Date")
                        else regexp_replace(btrim("Milestone_1_Actual_Date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Milestone_1_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as m1_actual,

        case
            when
                btrim("Milestone_2_Planned_Date") <> ''
                and (
                    btrim("Milestone_2_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Milestone_2_Planned_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Milestone_2_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Milestone_2_Planned_Date")
                        else regexp_replace(btrim("Milestone_2_Planned_Date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Milestone_2_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as m2_planned,
        case
            when
                btrim("Milestone_2_Actual_Date") <> ''
                and (
                    btrim("Milestone_2_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Milestone_2_Actual_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Milestone_2_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Milestone_2_Actual_Date")
                        else regexp_replace(btrim("Milestone_2_Actual_Date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Milestone_2_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as m2_actual,

        case
            when
                btrim("Milestone_3_Planned_Date") <> ''
                and (
                    btrim("Milestone_3_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Milestone_3_Planned_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Milestone_3_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Milestone_3_Planned_Date")
                        else regexp_replace(btrim("Milestone_3_Planned_Date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Milestone_3_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as m3_planned,
        case
            when
                btrim("Milestone_3_Actual_Date") <> ''
                and (
                    btrim("Milestone_3_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Milestone_3_Actual_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Milestone_3_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Milestone_3_Actual_Date")
                        else regexp_replace(btrim("Milestone_3_Actual_Date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Milestone_3_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as m3_actual,

        case
            when
                btrim("Milestone_4_Planned_Date") <> ''
                and (
                    btrim("Milestone_4_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Milestone_4_Planned_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Milestone_4_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Milestone_4_Planned_Date")
                        else regexp_replace(btrim("Milestone_4_Planned_Date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Milestone_4_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as m4_planned,
        case
            when
                btrim("Milestone_4_Actual_Date") <> ''
                and (
                    btrim("Milestone_4_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Milestone_4_Actual_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Milestone_4_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Milestone_4_Actual_Date")
                        else regexp_replace(btrim("Milestone_4_Actual_Date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Milestone_4_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as m4_actual,

        case
            when
                btrim("Milestone_5_Planned_Date") <> ''
                and (
                    btrim("Milestone_5_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Milestone_5_Planned_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Milestone_5_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Milestone_5_Planned_Date")
                        else regexp_replace(btrim("Milestone_5_Planned_Date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Milestone_5_Planned_Date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as m5_planned,
        case
            when
                btrim("Milestone_5_Actual_Date") <> ''
                and (
                    btrim("Milestone_5_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$'
                    or btrim("Milestone_5_Actual_Date") ~ '^\d{1,2}-[A-Za-z]{3}\s*-\s*\d{4}$'
                )
                then to_date(
                    case
                        when btrim("Milestone_5_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$' then btrim("Milestone_5_Actual_Date")
                        else regexp_replace(btrim("Milestone_5_Actual_Date"), '\\s*-\\s*', '-', 'g')
                    end,
                    case
                        when btrim("Milestone_5_Actual_Date") ~ '^\d{4}-\d{2}-\d{2}$' then 'YYYY-MM-DD'
                        else 'DD-Mon-YYYY'
                    end
                )
        end as m5_actual,

        -- PROJECT COUNTS (keep as numeric if possible)
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
            when btrim("No_of_Solar_panel_work_Installed___Project_details_") ~ '^\d+$'
                then "No_of_Solar_panel_work_Installed___Project_details_"::integer
        end as solar_panels,

        case
            when btrim("Others___Project_details_") ~ '^\d+$'
                then "Others___Project_details_"::integer
        end as other_project_counts,

        -- TEXT REMAINING
        coalesce(initcap(btrim("Post_completion_monitored_report_Link")), '') as monitored_report_link
        -- coalesce(initcap(btrim("")), '') as other_details

    from {{ source('iginteplus_25_26', 'Ignite__Master_Data_25_26') }}

),

with_delays as (

    select 
        sl_no,
        financial_year,
        quarter,
        -- remarks,
        district,
        stem_stp,
        fellowship,
        eco_champ,
        school_type,
        -- folder_link,    
        project_status,
        school_count,
        csr_partner,
        school_name,
        project_execution_budget,
        total_project_budget,
        student_count,
        starting_level_wash,
        current_level_wash,
        starting_level_classroom,
        current_level_classroom,
        funds_received_date,
        confirmation_date,
        m1_planned,
        m1_actual,
        m2_planned,
        m2_actual,
        m3_planned,
        m3_actual,
        m4_planned,
        m4_actual,  
        m5_planned,
        m5_actual,  
        case
            when m1_planned is null or m1_actual is null then null
            when (m1_actual - m1_planned) = 0 then 100.0
            when (m1_actual - m1_planned) >= 1 then 100.0 - (((m1_actual - m1_planned)::numeric / 30) * 100)
            when (m1_actual - m1_planned) <= -1 then 100.0 + ((m1_actual - m1_planned)::numeric / 30) * 100
        end as m1_delay_percent,
        case
            when m2_planned is null or m2_actual is null then null
            when (m2_actual - m2_planned) = 0 then 100.0
            when (m2_actual - m2_planned) >= 1 then 100.0 - (((m2_actual - m2_planned)::numeric / 30) * 100)
            when (m2_actual - m2_planned) <= -1 then 100.0 + ((m2_actual - m2_planned)::numeric / 30) * 100
        end as m2_delay_percent,
        case
            when m3_planned is null or m3_actual is null then null
            when (m3_actual - m3_planned) = 0 then 100.0
            when (m3_actual - m3_planned) >= 1 then 100.0 - (((m3_actual - m3_planned)::numeric / 30) * 100)
            when (m3_actual - m3_planned) <= -1 then 100.0 + ((m3_actual - m3_planned)::numeric / 30) * 100
        end as m3_delay_percent,
        case
            when m4_planned is null or m4_actual is null then null
            when (m4_actual - m4_planned) = 0 then 100.0
            when (m4_actual - m4_planned) >= 1 then 100.0 - (((m4_actual - m4_planned)::numeric / 30) * 100)
            when (m4_actual - m4_planned) <= -1 then 100.0 + ((m4_actual - m4_planned)::numeric / 30) * 100
        end as m4_delay_percent,
        case
            when m5_planned is null or m5_actual is null then null
            when (m5_actual - m5_planned) = 0 then 100.0
            when (m5_actual - m5_planned) >= 1 then 100.0 - (((m5_actual - m5_planned)::numeric / 30) * 100)
            when (m5_actual - m5_planned) <= -1 then 100.0 + ((m5_actual - m5_planned)::numeric / 30) * 100
        end as m5_delay_percent,
        washroom_constructed,
        washroom_renovated,
        classroom_constructed,
        classroom_renovated,
        furniture_count,
        ro_plants,
        solar_panels,
        monitored_report_link,
        other_project_counts

    from master

)

select 
    sl_no,
    financial_year,
    quarter,
    -- remarks,
    district,
    stem_stp,
    fellowship,
    eco_champ,
    school_type,
    -- folder_link,    
    project_status,
    school_count,
    csr_partner,
    school_name,
    project_execution_budget,
    total_project_budget,
    student_count,
    starting_level_wash,
    current_level_wash,
    starting_level_classroom,
    current_level_classroom,
    funds_received_date,
    confirmation_date,
    m1_planned,
    m1_actual,
    m2_planned,
    m2_actual,
    m3_planned,
    m3_actual,
    m4_planned,
    m4_actual,  
    m5_planned,
    m5_actual,  
    m1_delay_percent,
    m2_delay_percent,
    m3_delay_percent,
    m4_delay_percent,
    m5_delay_percent,
    (coalesce(m1_delay_percent, 0) + coalesce(m2_delay_percent, 0) + coalesce(m3_delay_percent, 0) + coalesce(m4_delay_percent, 0) + coalesce(m5_delay_percent, 0)) / nullif(
        (case when m1_delay_percent is not null then 1 else 0 end)
        + (case when m2_delay_percent is not null then 1 else 0 end)
        + (case when m3_delay_percent is not null then 1 else 0 end)
        + (case when m4_delay_percent is not null then 1 else 0 end)
        + (case when m5_delay_percent is not null then 1 else 0 end), 0
    ) as avg_delay_percent,
    washroom_constructed,
    washroom_renovated,
    classroom_constructed,
    classroom_renovated,
    furniture_count,
    ro_plants,
    solar_panels,
    monitored_report_link,
    other_project_counts

from with_delays
where school_name <> '' and student_count is not null
