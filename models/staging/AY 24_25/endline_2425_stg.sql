with endline as (
    select
        Coalesce(Initcap(Btrim("City"::text)), '') as "City",
        Coalesce(Initcap(Btrim("PM_Name"::text)), '') as "PM Name",
        Coalesce(Initcap(Btrim("School_Name"::text)), '') as "School Name",
        Coalesce(Initcap(Btrim("Classroom_ID"::text)), '') as "Classroom ID",
        Coalesce(Initcap(Btrim("Fellow_Name"::text)), '') as "Fellow Name",
        case when Btrim("Cohort"::text) ~ '^\d+$' then ("Cohort"::text)::integer end as "Cohort",
        case when Btrim("Grade_Taught"::text) ~ '^\d+$' then ("Grade_Taught"::text)::integer end as "Grade Taught",
        Coalesce(Btrim("Student_ID"::text), '') as "Student ID",
        Coalesce(Initcap(Btrim("Student_Name"::text)), '') as "Student Name",
        Coalesce(Btrim("Endline_RC_Level"::text), '') as "Endline RC Level",
        Coalesce(Btrim("Endline_RC_Grade_Level"::text), '') as "Endline RC Grade Level",
        Coalesce(Initcap(Btrim("Endline_RC_Status"::text)), '') as "Endline RC Status",
        Coalesce(Initcap(Btrim("Endline_RF_Status"::text)), '') as "Endline RF Status",
        Coalesce(Initcap(Btrim("Endline_Math_Level"::text)), '') as "Endline Math Level",
        Coalesce(Initcap(Btrim("Endline_Math_Status"::text)), '') as "Endline Math Status",
        case
            when Lower(Btrim("Endline_Math_Mastery"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Endline_Math_Mastery"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Endline_Math_Mastery"::text), '%','')::numeric
            when Btrim("Endline_Math_Mastery"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math_Mastery"::text)::numeric
        end as "Endline Math Mastery",
        case
            when Lower(Btrim("Endline_Math___in_Numbers"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Endline_Math___in_Numbers"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Endline_Math___in_Numbers"::text), '%','')::numeric
            when Btrim("Endline_Math___in_Numbers"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Numbers"::text)::numeric
        end as "Endline Math % in Numbers",
        case
            when Lower(Btrim("Endline_Math___in_Patterns"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Endline_Math___in_Patterns"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Endline_Math___in_Patterns"::text), '%','')::numeric
            when Btrim("Endline_Math___in_Patterns"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Patterns"::text)::numeric
        end as "Endline Math % in Patterns",
        case
            when Lower(Btrim("Endline_Math___in_Geometry"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Endline_Math___in_Geometry"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Endline_Math___in_Geometry"::text), '%','')::numeric
            when Btrim("Endline_Math___in_Geometry"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Geometry"::text)::numeric
        end as "Endline Math % in Geometry",
        case
            when Lower(Btrim("Endline_Math___in_Mensuration"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Endline_Math___in_Mensuration"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Endline_Math___in_Mensuration"::text), '%','')::numeric
            when Btrim("Endline_Math___in_Mensuration"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Mensuration"::text)::numeric
        end as "Endline Math % in Mensuration",
        case
            when Lower(Btrim("Endline_Math___in_Time"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Endline_Math___in_Time"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Endline_Math___in_Time"::text), '%','')::numeric
            when Btrim("Endline_Math___in_Time"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Time"::text)::numeric
        end as "Endline Math % in Time",
        case
            when Lower(Btrim("Endline_Math___in_Operations"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Endline_Math___in_Operations"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Endline_Math___in_Operations"::text), '%','')::numeric
            when Btrim("Endline_Math___in_Operations"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Operations"::text)::numeric
        end as "Endline Math % in Operations",
        case
            when Lower(Btrim("Endline_Math___in_Data_Handling"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Endline_Math___in_Data_Handling"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Endline_Math___in_Data_Handling"::text), '%','')::numeric
            when Btrim("Endline_Math___in_Data_Handling"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Data_Handling"::text)::numeric
        end as "Endline Math % in Data Handling",
        case
            when Lower(Btrim("Factual"::text)) in ('', 'na') then null
            when Btrim("Factual"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Factual"::text),'%','')::numeric
            when Btrim("Factual"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Factual"::text)::numeric
        end as "Factual",
        case
            when Lower(Btrim("Inference"::text)) in ('', 'na') then null
            when Btrim("Inference"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Inference"::text),'%','')::numeric
            when Btrim("Inference"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Inference"::text)::numeric
        end as "Inference",
        case
            when Lower(Btrim("Critical_Thinking"::text)) in ('', 'na') then null
            when Btrim("Critical_Thinking"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Critical_Thinking"::text),'%','')::numeric
            when Btrim("Critical_Thinking"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Critical_Thinking"::text)::numeric
        end as "Critical Thinking",
        case
            when Lower(Btrim("Vocabulary"::text)) in ('', 'na') then null
            when Btrim("Vocabulary"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Vocabulary"::text),'%','')::numeric
            when Btrim("Vocabulary"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Vocabulary"::text)::numeric
        end as "Vocabulary",
        case
            when Lower(Btrim("Grammar"::text)) in ('', 'na') then null
            when Btrim("Grammar"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Grammar"::text),'%','')::numeric
            when Btrim("Grammar"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Grammar"::text)::numeric
        end as "Grammar",
        case
            when Lower(Btrim("Letter_sounds"::text)) in ('', 'na') then null
            when Btrim("Letter_sounds"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Letter_sounds"::text),'%','')::numeric
            when Btrim("Letter_sounds"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Letter_sounds"::text)::numeric
        end as "Letter sounds",
        case
            when Lower(Btrim("CVC_words"::text)) in ('', 'na') then null
            when Btrim("CVC_words"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("CVC_words"::text),'%','')::numeric
            when Btrim("CVC_words"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("CVC_words"::text)::numeric
        end as "CVC words",
        case
            when Lower(Btrim("Blends"::text)) in ('', 'na') then null
            when Btrim("Blends"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Blends"::text),'%','')::numeric
            when Btrim("Blends"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Blends"::text)::numeric
        end as "Blends",
        case
            when Lower(Btrim("Consonant_diagraph"::text)) in ('', 'na') then null
            when Btrim("Consonant_diagraph"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Consonant_diagraph"::text),'%','')::numeric
            when Btrim("Consonant_diagraph"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Consonant_diagraph"::text)::numeric
        end as "Consonant diagraph",
        case
            when Lower(Btrim("Magic_E_words"::text)) in ('', 'na') then null
            when Btrim("Magic_E_words"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Magic_E_words"::text),'%','')::numeric
            when Btrim("Magic_E_words"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Magic_E_words"::text)::numeric
        end as "Magic E words",
        case
            when Lower(Btrim("Vowel_diagraphs"::text)) in ('', 'na') then null
            when Btrim("Vowel_diagraphs"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Vowel_diagraphs"::text),'%','')::numeric
            when Btrim("Vowel_diagraphs"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Vowel_diagraphs"::text)::numeric
        end as "Vowel diagraphs",
        case
            when Lower(Btrim("Multi_syllabelle_words"::text)) in ('', 'na') then null
            when Btrim("Multi_syllabelle_words"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Multi_syllabelle_words"::text),'%','')::numeric
            when Btrim("Multi_syllabelle_words"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Multi_syllabelle_words"::text)::numeric
        end as "Multi syllabelle words",
        case
            when Lower(Btrim("Passage_1"::text)) in ('', 'na') then null
            when Btrim("Passage_1"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Passage_1"::text),'%','')::numeric
            when Btrim("Passage_1"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Passage_1"::text)::numeric
        end as "Passage 1",
        case
            when Lower(Btrim("Passage_2"::text)) in ('', 'na') then null
            when Btrim("Passage_2"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Passage_2"::text),'%','')::numeric
            when Btrim("Passage_2"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Passage_2"::text)::numeric
        end as "Passage 2",
        case
            when Lower(Btrim("RF_"::text)) in ('', 'na') then null
            when Btrim("RF_"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("RF_"::text),'%','')::numeric
            when Btrim("RF_"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("RF_"::text)::numeric
        end as "RF %",
        case when Btrim("Developing"::text) ~ '^\d+$' then ("Developing"::text)::integer end as "Developing",
        case when Btrim("Beginner"::text) ~ '^\d+$' then ("Beginner"::text)::integer end as "Beginner",
        case when Btrim("Intermediate"::text) ~ '^\d+$' then ("Intermediate"::text)::integer end as "Intermediate",
        case when Btrim("Advanced"::text) ~ '^\d+$' then ("Advanced"::text)::integer end as "Advanced",
        case
            when Btrim("RC_Assessed___"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("RC_Assessed___"::text),'%','')
            when Btrim("RC_Assessed___"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("RC_Assessed___"::text)
            else Nullif(Initcap(Btrim("RC_Assessed___"::text)), '')
        end as "RC Assessed %.",
        case when Btrim("Endline_RF_Code"::text) ~ '^\d+$' then ("Endline_RF_Code"::text)::integer end as "Endline RF Code"
    from {{ source('fellowship_24_25_data', 'Raw_Data_Endline') }}
)

select distinct
    e."Student ID" as student_id_end,
    e."City" as city_end,
    e."PM Name" as pm_name_end,
    e."School Name" as school_name_end,
    e."Classroom ID" as classroom_id_end,
    e."Fellow Name" as fellow_name_end,
    e."Cohort" as cohort_end,
    e."Grade Taught" as grade_taught_end,
    e."Student Name" as student_name_end,
    e."Endline RC Level" as rc_level_end,
    e."Endline RC Grade Level" as rc_grade_level_end, 
    e."Endline RC Status" as rc_status_end,
    e."Endline RF Status" as rf_status_end,
    e."Endline Math Level" as math_level_end,
    e."Endline Math Status" as math_status_end,
    e."Endline Math Mastery" as math_mastery_end,
    e."Endline Math % in Numbers" as math_perc_numbers_end,
    e."Endline Math % in Patterns" as math_perc_patterns_end,
    e."Endline Math % in Geometry" as math_perc_geometry_end,
    e."Endline Math % in Mensuration" as math_perc_mensuration_end,
    e."Endline Math % in Time" as math_perc_time_end,
    e."Endline Math % in Operations" as math_perc_operations_end,
    e."Endline Math % in Data Handling" as math_perc_data_handling_end,
    e."Factual" as factual_end,
    e."Inference" as inference_end,
    e."Critical Thinking" as critical_thinking_end,
    e."Vocabulary" as vocabulary_end,
    e."Grammar" as grammar_end,
    e."Letter sounds" as letter_sounds_end,
    e."CVC words" as cvc_words_end,
    e."Blends" as blends_end,
    e."Consonant diagraph" as consonant_diagraph_end,
    e."Magic E words" as magic_e_words_end,
    e."Vowel diagraphs" as vowel_diagraphs_end,
    e."Multi syllabelle words" as multi_syllabelle_words_end,
    e."Passage 1" as passage_1_end,
    e."Passage 2" as passage_2_end,
    e."RF %" as rf_perc_end,
    e."Developing" as developing_end,
    e."Beginner" as beginner_end,
    e."Intermediate" as intermediate_end,
    e."Advanced" as advanced_end,
    e."RC Assessed %." as rc_assessed_perc_end,
    e."Endline RF Code" as rf_code_end
from endline as e
where e."Student ID" <> ''
