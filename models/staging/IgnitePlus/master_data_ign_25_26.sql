{{
    config(
        materialized = 'view'
    )
}}

with source as (

    select * from {{ source('iginteplus_25_26', 'Ignite__Master_Data_25_26') }}

),

cleaned as (

    select
        -- INTEGER
        case
            when btrim("Sl_No") ~ '^\d+$'
            then "Sl_No"::integer
        end                                                                         as sl_no,

        -- TEXT
        coalesce(initcap(btrim("Quarter")),              '')                        as quarter,
        coalesce(initcap(btrim("District")),             '')                        as district,
        coalesce(initcap(btrim("STEM_STP")),             '')                        as stem_stp,
        coalesce(initcap(btrim("Fellowship")),           '')                        as fellowship,
        coalesce(initcap(btrim("Eco_Champ")),            '')                        as eco_champ,
        coalesce(initcap(btrim("Type_of_School")),       '')                        as school_type,
        coalesce(initcap(btrim("Financial_Year")),       '')                        as financial_year,
        coalesce(initcap(btrim("Project_Status")),       '')                        as project_status,
        coalesce(initcap(btrim("Name_of_CSR_Partner")),  '')                        as csr_partner,
        coalesce(initcap(btrim("Name_of_school___Location")), '')                   as school_name,

        -- NUMERIC
        case
            when btrim("School_count") ~ '^\d+(\.\d+)?$'
            then "School_count"::numeric
        end                                                                         as school_count,

        case
            when btrim("Actual_Considerable_Student_count__As_per_standard_norms___thro") ~ '^\d+$'
            then "Actual_Considerable_Student_count__As_per_standard_norms___thro"::integer
        end                                                                         as student_count,

        -- BUDGETS: strip currency symbols, commas, whitespace; take first tab-split chunk
        case
            when btrim("Project_execution_Budget__MOU_") <> ''
            then regexp_replace(
                    split_part(btrim("Project_execution_Budget__MOU_"), E'\t', 1),
                    '₹|,|\s', '', 'g'
                 )::numeric
        end                                                                         as project_execution_budget,

        case
            when btrim("Total_project_budget__With_Branding__PM___Admin_cost___MOU_") <> ''
            then regexp_replace(
                    split_part(btrim("Total_project_budget__With_Branding__PM___Admin_cost___MOU_"), E'\t', 1),
                    '₹|,|\s', '', 'g'
                 )::numeric
        end                                                                         as total_project_budget,

        -- LEVELS
        coalesce(initcap(btrim("Starting_Level___WASH")),       '')                 as starting_level_wash,
        coalesce(initcap(btrim("Current_Level___WASH")),        '')                 as current_level_wash,
        coalesce(initcap(btrim("Starting_Level___Classroom")),  '')                 as starting_level_classroom,
        coalesce(initcap(btrim("Current_Level___Classroom")),   '')                 as current_level_classroom,

        -- KEY DATES
        {{ parse_date_field('"Date_of_funds_received"') }}                          as funds_received_date,
        {{ parse_date_field('"Date_of_Confirmation_from_CR_team"') }}               as confirmation_date,

        -- MILESTONE DATES
        {{ parse_date_field('"Milestone_1_Planned_Date"') }}                        as m1_planned,
        {{ parse_date_field('"Milestone_1_Actual_Date"') }}                         as m1_actual,
        {{ parse_date_field('"Milestone_2_Planned_Date"') }}                        as m2_planned,
        {{ parse_date_field('"Milestone_2_Actual_Date"') }}                         as m2_actual,
        {{ parse_date_field('"Milestone_3_Planned_Date"') }}                        as m3_planned,
        {{ parse_date_field('"Milestone_3_Actual_Date"') }}                         as m3_actual,
        {{ parse_date_field('"Milestone_4_Planned_Date"') }}                        as m4_planned,
        {{ parse_date_field('"Milestone_4_Actual_Date"') }}                         as m4_actual,
        {{ parse_date_field('"Milestone_5_Planned_Date"') }}                        as m5_planned,
        {{ parse_date_field('"Milestone_5_Actual_Date"') }}                         as m5_actual,

        -- PROJECT COUNTS
        case when btrim("No_of_Washroom_Constructred__Project_details_")   ~ '^\d+$'
             then "No_of_Washroom_Constructred__Project_details_"::integer  end     as washroom_constructed,
        case when btrim("No_of_Washroom_Renovated___Project_details_")     ~ '^\d+$'
             then "No_of_Washroom_Renovated___Project_details_"::integer    end     as washroom_renovated,
        case when btrim("No_of_Classroom_Constructred___Project_details_") ~ '^\d+$'
             then "No_of_Classroom_Constructred___Project_details_"::integer end    as classroom_constructed,
        case when btrim("No_of_Classroom_Renovated___Project_details_")    ~ '^\d+$'
             then "No_of_Classroom_Renovated___Project_details_"::integer   end     as classroom_renovated,
        case when btrim("No_of_furnitures_provided___Project_details_")    ~ '^\d+$'
             then "No_of_furnitures_provided___Project_details_"::integer   end     as furniture_count,
        case when btrim("No_of_RO_Plant_Installed___Project_details_")     ~ '^\d+$'
             then "No_of_RO_Plant_Installed___Project_details_"::integer    end     as ro_plants,
        case when btrim("No_of_Solar_panel_work_Installed___Project_details_") ~ '^\d+$'
             then "No_of_Solar_panel_work_Installed___Project_details_"::integer end as solar_panels,
        case when btrim("Others___Project_details_")                       ~ '^\d+$'
             then "Others___Project_details_"::integer                      end     as other_project_counts,

        coalesce(initcap(btrim("Post_completion_monitored_report_Link")), '')       as monitored_report_link

    from source

)

select * from cleaned where school_name <> '' and student_count is not null