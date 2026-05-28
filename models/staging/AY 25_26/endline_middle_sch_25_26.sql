with endline as (
    select
        COALESCE(INITCAP(BTRIM("PM"::TEXT)), '') as "PM",
        case when BTRIM("Cohort"::TEXT) ~ '^\d+$' then ("Cohort"::TEXT)::INTEGER end as "Cohort",
        COALESCE(BTRIM("Student_ID"::TEXT), '') as "Student ID",
        COALESCE(INITCAP(BTRIM("Fellow_name"::TEXT)), '') as "Fellow Name",
        COALESCE(INITCAP(BTRIM("School_Name"::TEXT)), '') as "School Name",
        COALESCE(BTRIM("Classroom_ID"::TEXT), '') as "Classroom ID",
        COALESCE(INITCAP(BTRIM("Student_name"::TEXT)), '') as "Student Name",
        case when BTRIM("Student_grade"::TEXT) ~ '^\d+$' then ("Student_grade"::TEXT)::INTEGER end as "Student Grade",

        -- Math Endline Scores
        case when BTRIM("Number_sense_Math_Endline"::TEXT) ~ '^[0-9.]+%?$' then REPLACE("Number_sense_Math_Endline"::TEXT, '%', '')::NUMERIC end as "Number Sense Math Endline",
        case when BTRIM("Geometry_Math_Endline"::TEXT) ~ '^[0-9.]+%?$' then REPLACE("Geometry_Math_Endline"::TEXT, '%', '')::NUMERIC end as "Geometry Math Endline",
        case when BTRIM("Data_Math_Endline"::TEXT) ~ '^[0-9.]+%?$' then REPLACE("Data_Math_Endline"::TEXT, '%', '')::NUMERIC end as "Data Math Endline",
        case when BTRIM("Algebra_Math_Endline"::TEXT) ~ '^[0-9.]+%?$' then REPLACE("Algebra_Math_Endline"::TEXT, '%', '')::NUMERIC end as "Algebra Math Endline",
        case when BTRIM("Total_Math_Endline"::TEXT) ~ '^[0-9.]+%?$' then REPLACE("Total_Math_Endline"::TEXT, '%', '')::NUMERIC end as "Total Math Endline",
        COALESCE(BTRIM("Grade_level_Math_Endline"::TEXT), '') as "Grade Level Math Endline",

        -- Science Endline Scores
        case when BTRIM("Matter_Sci_Endline"::TEXT) ~ '^[0-9.]+%?$' then REPLACE("Matter_Sci_Endline"::TEXT, '%', '')::NUMERIC end as "Matter Sci Endline",
        case when BTRIM("Measurements_Sci_Endline"::TEXT) ~ '^[0-9.]+%?$' then REPLACE("Measurements_Sci_Endline"::TEXT, '%', '')::NUMERIC end as "Measurements Sci Endline",
        case when BTRIM("Force___Motion_Sci_Endline"::TEXT) ~ '^[0-9.]+%?$' then REPLACE("Force___Motion_Sci_Endline"::TEXT, '%', '')::NUMERIC end as "Force & Motion Sci Endline",
        case when BTRIM("Electricity___Magnetism_Sci_Endline"::TEXT) ~ '^[0-9.]+%?$' then REPLACE("Electricity___Magnetism_Sci_Endline"::TEXT, '%', '')::NUMERIC end as "Electricity & Magnetism Sci Endline",
        case when BTRIM("Light_Sci_Endline"::TEXT) ~ '^[0-9.]+%?$' then REPLACE("Light_Sci_Endline"::TEXT, '%', '')::NUMERIC end as "Light Sci Endline",
        case when BTRIM("Plants___Animals_Sci_Endline"::TEXT) ~ '^[0-9.]+%?$' then REPLACE("Plants___Animals_Sci_Endline"::TEXT, '%', '')::NUMERIC end as "Plants & Animals Sci Endline",
        case when BTRIM("Total_Sci_Endline"::TEXT) ~ '^[0-9.]+%?$' then REPLACE("Total_Sci_Endline"::TEXT, '%', '')::NUMERIC end as "Total Sci Endline",
        COALESCE(BTRIM("Grade_level_Sci_Endline"::TEXT), '') as "Grade Level Sci Endline"

    from {{ source('fellowship_25_26', 'endline_middle_school_data_25_26') }}
)

select distinct
    e."Student ID"              as student_id_end,
    e."PM"                      as pm_end,
    e."Cohort"                  as cohort_end,
    e."Fellow Name"             as fellow_name_end,
    e."School Name"             as school_name_end,
    e."Classroom ID"            as classroom_id_end,
    e."Student Name"            as student_name_end,
    e."Student Grade"           as student_grade_end,

    -- Math
    e."Number Sense Math Endline"           as number_sense_math_endline,
    e."Geometry Math Endline"               as geometry_math_endline,
    e."Data Math Endline"                   as data_math_endline,
    e."Algebra Math Endline"                as algebra_math_endline,
    e."Total Math Endline"                  as total_math_endline,
    e."Grade Level Math Endline"            as grade_level_math_endline,

    -- Science
    e."Matter Sci Endline"                  as matter_sci_endline,
    e."Measurements Sci Endline"            as measurements_sci_endline,
    e."Force & Motion Sci Endline"          as force_motion_sci_endline,
    e."Electricity & Magnetism Sci Endline" as electricity_magnetism_sci_endline,
    e."Light Sci Endline"                   as light_sci_endline,
    e."Plants & Animals Sci Endline"        as plants_animals_sci_endline,
    e."Total Sci Endline"                   as total_sci_endline,
    e."Grade Level Sci Endline"             as grade_level_sci_endline

from endline as e
where e."Student ID" <> ''
