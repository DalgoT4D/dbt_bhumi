with baseline as (
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
        Coalesce(Btrim("Baseline_RC_Level"::text), '') as "Baseline RC Level",
        Coalesce(Btrim("Basel_RC_Grade_Level"::text), '') as "Baseline RC Grade Level",
        Coalesce(Initcap(Btrim("Baseline_RC_Status"::text)), '') as "Baseline RC Status",
        Coalesce(Initcap(Btrim("Baseline_RF_Status"::text)), '') as "Baseline RF Status",
        Coalesce(Initcap(Btrim("Baseline_Math_Level"::text)), '') as "Baseline Math Level",
        Coalesce(Initcap(Btrim("Baseline_Math_Status"::text)), '') as "Baseline Math Status",
        case
            when Lower(Btrim("Baseline_Math_Mastery"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Baseline_Math_Mastery"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Baseline_Math_Mastery"::text), '%','')::numeric
            when Btrim("Baseline_Math_Mastery"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math_Mastery"::text)::numeric
        end as "Baseline Math Mastery",
        case
            when Lower(Btrim("Baseline_Math___in_Numbers"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Baseline_Math___in_Numbers"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Baseline_Math___in_Numbers"::text), '%','')::numeric
            when Btrim("Baseline_Math___in_Numbers"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Numbers"::text)::numeric
        end as "Baseline Math % in Numbers",
        case
            when Lower(Btrim("Baseline_Math___in_Patterns"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Baseline_Math___in_Patterns"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Baseline_Math___in_Patterns"::text), '%','')::numeric
            when Btrim("Baseline_Math___in_Patterns"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Patterns"::text)::numeric
        end as "Baseline Math % in Patterns",
        case
            when Lower(Btrim("Baseline_Math___in_Geometry"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Baseline_Math___in_Geometry"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Baseline_Math___in_Geometry"::text), '%','')::numeric
            when Btrim("Baseline_Math___in_Geometry"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Geometry"::text)::numeric
        end as "Baseline Math % in Geometry",
        case
            when Lower(Btrim("Baseline_Math___in_Mensuration"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Baseline_Math___in_Mensuration"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Baseline_Math___in_Mensuration"::text), '%','')::numeric
            when Btrim("Baseline_Math___in_Mensuration"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Mensuration"::text)::numeric
        end as "Baseline Math % in Mensuration",
        case
            when Lower(Btrim("Baseline_Math___in_Time"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Baseline_Math___in_Time"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Baseline_Math___in_Time"::text), '%','')::numeric
            when Btrim("Baseline_Math___in_Time"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Time"::text)::numeric
        end as "Baseline Math % in Time",
        case
            when Lower(Btrim("Baseline_Math___in_Operations"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Baseline_Math___in_Operations"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Baseline_Math___in_Operations"::text), '%','')::numeric
            when Btrim("Baseline_Math___in_Operations"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Operations"::text)::numeric
        end as "Baseline Math % in Operations",
        case
            when Lower(Btrim("Baseline_Math___in_Data_Handling"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
            when Btrim("Baseline_Math___in_Data_Handling"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then Replace(Btrim("Baseline_Math___in_Data_Handling"::text), '%','')::numeric
            when Btrim("Baseline_Math___in_Data_Handling"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Data_Handling"::text)::numeric
        end as "Baseline Math % in Data Handling",
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
        case when Btrim("Baseline_RF_Code"::text) ~ '^\d+$' then ("Baseline_RF_Code"::text)::integer end as "Baseline RF Code"
    from {{ source('fellowship_24_25_data', 'Raw_Data_Baseline') }}
)

select distinct
    b."Student ID" as student_id_base,
    b."City" as city_base,
    b."PM Name" as pm_name_base,
    b."School Name" as school_name_base,
    b."Classroom ID" as classroom_id_base,
    b."Fellow Name" as fellow_name_base,
    b."Cohort" as cohort_base,
    b."Grade Taught" as grade_taught_base,
    b."Student Name" as student_name_base,
    b."Baseline RC Level" as rc_level_base,
    b."Baseline RC Grade Level" as rc_grade_level_base, 
    b."Baseline RC Status" as rc_status_base,
    b."Baseline RF Status" as rf_status_base,
    b."Baseline Math Level" as math_level_base,
    b."Baseline Math Status" as math_status_base,
    b."Baseline Math Mastery" as math_mastery_base,
    b."Baseline Math % in Numbers" as math_perc_numbers_base,
    b."Baseline Math % in Patterns" as math_perc_patterns_base,
    b."Baseline Math % in Geometry" as math_perc_geometry_base,
    b."Baseline Math % in Mensuration" as math_perc_mensuration_base,
    b."Baseline Math % in Time" as math_perc_time_base,
    b."Baseline Math % in Operations" as math_perc_operations_base,
    b."Baseline Math % in Data Handling" as math_perc_data_handling_base,
    b."Factual" as factual_base,
    b."Inference" as inference_base,
    b."Critical Thinking" as critical_thinking_base,
    b."Vocabulary" as vocabulary_base,
    b."Grammar" as grammar_base,
    b."Letter sounds" as letter_sounds_base,
    b."CVC words" as cvc_words_base,
    b."Blends" as blends_base,
    b."Consonant diagraph" as consonant_diagraph_base,
    b."Magic E words" as magic_e_words_base,
    b."Vowel diagraphs" as vowel_diagraphs_base,
    b."Multi syllabelle words" as multi_syllabelle_words_base,
    b."Passage 1" as passage_1_base,
    b."Passage 2" as passage_2_base,
    b."RF %" as rf_perc_base,
    b."Developing" as developing_base,
    b."Beginner" as beginner_base,
    b."Intermediate" as intermediate_base,
    b."Advanced" as advanced_base,
    b."RC Assessed %." as rc_assessed_perc_base,
    b."Baseline RF Code" as rf_code_base
from baseline as b
where b."Student ID" <> ''
