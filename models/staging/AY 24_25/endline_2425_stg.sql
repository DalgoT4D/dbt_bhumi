with endline as (
  select
    Coalesce(initcap(btrim("City"::text)), '') as "City",
    Coalesce(initcap(btrim("PM_Name"::text)), '') as "PM Name",
    Coalesce(initcap(btrim("School_Name"::text)), '') as "School Name",
    Coalesce(initcap(btrim("Classroom_ID"::text)), '') as "Classroom ID",
    Coalesce(initcap(btrim("Fellow_Name"::text)), '') as "Fellow Name",
    case when btrim("Cohort"::text) ~ '^\d+$' then ("Cohort"::text)::integer else null end as "Cohort",
    case when btrim("Grade_Taught"::text) ~ '^\d+$' then ("Grade_Taught"::text)::integer else null end as "Grade Taught",
    Coalesce(btrim("Student_ID"::text), '') as "Student ID",
    Coalesce(initcap(btrim("Student_Name"::text)), '') as "Student Name",
    Coalesce(btrim("Endline_RC_Level"::text), '') as "Endline RC Level",
    Coalesce(btrim("Endline_RC_Grade_Level"::text), '') as "Endline RC Grade Level",
    Coalesce(initcap(btrim("Endline_RC_Status"::text)), '') as "Endline RC Status",
    Coalesce(initcap(btrim("Endline_RF_Status"::text)), '') as "Endline RF Status",
    Coalesce(initcap(btrim("Endline_Math_Level"::text)), '') as "Endline Math Level",
    Coalesce(initcap(btrim("Endline_Math_Status"::text)), '') as "Endline Math Status",
    case when lower(btrim("Endline_Math_Mastery"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Endline_Math_Mastery"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Endline_Math_Mastery"::text), '%','')::numeric
         when btrim("Endline_Math_Mastery"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math_Mastery"::text)::numeric
         else null end as "Endline Math Mastery",
    case when lower(btrim("Endline_Math___in_Numbers"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Endline_Math___in_Numbers"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Endline_Math___in_Numbers"::text), '%','')::numeric
         when btrim("Endline_Math___in_Numbers"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Numbers"::text)::numeric
         else null end as "Endline Math % in Numbers",
    case when lower(btrim("Endline_Math___in_Patterns"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Endline_Math___in_Patterns"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Endline_Math___in_Patterns"::text), '%','')::numeric
         when btrim("Endline_Math___in_Patterns"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Patterns"::text)::numeric
         else null end as "Endline Math % in Patterns",
    case when lower(btrim("Endline_Math___in_Geometry"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Endline_Math___in_Geometry"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Endline_Math___in_Geometry"::text), '%','')::numeric
         when btrim("Endline_Math___in_Geometry"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Geometry"::text)::numeric
         else null end as "Endline Math % in Geometry",
    case when lower(btrim("Endline_Math___in_Mensuration"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Endline_Math___in_Mensuration"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Endline_Math___in_Mensuration"::text), '%','')::numeric
         when btrim("Endline_Math___in_Mensuration"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Mensuration"::text)::numeric
         else null end as "Endline Math % in Mensuration",
    case when lower(btrim("Endline_Math___in_Time"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Endline_Math___in_Time"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Endline_Math___in_Time"::text), '%','')::numeric
         when btrim("Endline_Math___in_Time"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Time"::text)::numeric
         else null end as "Endline Math % in Time",
    case when lower(btrim("Endline_Math___in_Operations"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Endline_Math___in_Operations"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Endline_Math___in_Operations"::text), '%','')::numeric
         when btrim("Endline_Math___in_Operations"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Operations"::text)::numeric
         else null end as "Endline Math % in Operations",
    case when lower(btrim("Endline_Math___in_Data_Handling"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Endline_Math___in_Data_Handling"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Endline_Math___in_Data_Handling"::text), '%','')::numeric
         when btrim("Endline_Math___in_Data_Handling"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Endline_Math___in_Data_Handling"::text)::numeric
         else null end as "Endline Math % in Data Handling",
    case when lower(btrim("Factual"::text)) in ('', 'na') then null
         when btrim("Factual"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Factual"::text),'%','')::numeric
         when btrim("Factual"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Factual"::text)::numeric
         else null end as "Factual",
    case when lower(btrim("Inference"::text)) in ('', 'na') then null
         when btrim("Inference"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Inference"::text),'%','')::numeric
         when btrim("Inference"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Inference"::text)::numeric
         else null end as "Inference",
    case when lower(btrim("Critical_Thinking"::text)) in ('', 'na') then null
         when btrim("Critical_Thinking"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Critical_Thinking"::text),'%','')::numeric
         when btrim("Critical_Thinking"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Critical_Thinking"::text)::numeric
         else null end as "Critical Thinking",
    case when lower(btrim("Vocabulary"::text)) in ('', 'na') then null
         when btrim("Vocabulary"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Vocabulary"::text),'%','')::numeric
         when btrim("Vocabulary"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Vocabulary"::text)::numeric
         else null end as "Vocabulary",
    case when lower(btrim("Grammar"::text)) in ('', 'na') then null
         when btrim("Grammar"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Grammar"::text),'%','')::numeric
         when btrim("Grammar"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Grammar"::text)::numeric
         else null end as "Grammar",
    case when lower(btrim("Letter_sounds"::text)) in ('', 'na') then null
         when btrim("Letter_sounds"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Letter_sounds"::text),'%','')::numeric
         when btrim("Letter_sounds"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Letter_sounds"::text)::numeric
         else null end as "Letter sounds",
    case when lower(btrim("CVC_words"::text)) in ('', 'na') then null
         when btrim("CVC_words"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("CVC_words"::text),'%','')::numeric
         when btrim("CVC_words"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("CVC_words"::text)::numeric
         else null end as "CVC words",
    case when lower(btrim("Blends"::text)) in ('', 'na') then null
         when btrim("Blends"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Blends"::text),'%','')::numeric
         when btrim("Blends"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Blends"::text)::numeric
         else null end as "Blends",
    case when lower(btrim("Consonant_diagraph"::text)) in ('', 'na') then null
         when btrim("Consonant_diagraph"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Consonant_diagraph"::text),'%','')::numeric
         when btrim("Consonant_diagraph"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Consonant_diagraph"::text)::numeric
         else null end as "Consonant diagraph",
    case when lower(btrim("Magic_E_words"::text)) in ('', 'na') then null
         when btrim("Magic_E_words"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Magic_E_words"::text),'%','')::numeric
         when btrim("Magic_E_words"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Magic_E_words"::text)::numeric
         else null end as "Magic E words",
    case when lower(btrim("Vowel_diagraphs"::text)) in ('', 'na') then null
         when btrim("Vowel_diagraphs"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Vowel_diagraphs"::text),'%','')::numeric
         when btrim("Vowel_diagraphs"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Vowel_diagraphs"::text)::numeric
         else null end as "Vowel diagraphs",
    case when lower(btrim("Multi_syllabelle_words"::text)) in ('', 'na') then null
         when btrim("Multi_syllabelle_words"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Multi_syllabelle_words"::text),'%','')::numeric
         when btrim("Multi_syllabelle_words"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Multi_syllabelle_words"::text)::numeric
         else null end as "Multi syllabelle words",
    case when lower(btrim("Passage_1"::text)) in ('', 'na') then null
         when btrim("Passage_1"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Passage_1"::text),'%','')::numeric
         when btrim("Passage_1"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Passage_1"::text)::numeric
         else null end as "Passage 1",
    case when lower(btrim("Passage_2"::text)) in ('', 'na') then null
         when btrim("Passage_2"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Passage_2"::text),'%','')::numeric
         when btrim("Passage_2"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Passage_2"::text)::numeric
         else null end as "Passage 2",
    case when lower(btrim("RF_"::text)) in ('', 'na') then null
         when btrim("RF_"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("RF_"::text),'%','')::numeric
         when btrim("RF_"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("RF_"::text)::numeric
         else null end as "RF %",
    case when btrim("Developing"::text) ~ '^\d+$' then ("Developing"::text)::integer else null end as "Developing",
    case when btrim("Beginner"::text) ~ '^\d+$' then ("Beginner"::text)::integer else null end as "Beginner",
    case when btrim("Intermediate"::text) ~ '^\d+$' then ("Intermediate"::text)::integer else null end as "Intermediate",
    case when btrim("Advanced"::text) ~ '^\d+$' then ("Advanced"::text)::integer else null end as "Advanced",
    case when btrim("RC_Assessed___"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("RC_Assessed___"::text),'%','')
         when btrim("RC_Assessed___"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("RC_Assessed___"::text)
         else nullif(initcap(btrim("RC_Assessed___"::text)), '') end as "RC Assessed %.",
    case when btrim("Endline_RF_Code"::text) ~ '^\d+$' then ("Endline_RF_Code"::text)::integer else null end as "Endline RF Code"
  from {{ source('fellowship_24_25_data', 'Raw_Data_Endline') }}
)

select distinct
  e."Student ID" as student_id_end,
  e."City" as city_end,
  e."PM Name" as PM_name_end,
  e."School Name" as school_name_end,
  e."Classroom ID" as classroom_id_end,
  e."Fellow Name" as fellow_name_end,
  e."Cohort" as cohort_end,
  e."Grade Taught" as grade_taught_end,
  e."Student Name" as student_name_end,
  e."Endline RC Level" as RC_level_end,
  e."Endline RC Grade Level" as RC_grade_level_end, 
  e."Endline RC Status" as RC_status_end,
  e."Endline RF Status" as RF_status_end,
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
  e."CVC words" as CVC_words_end,
  e."Blends" as blends_end,
  e."Consonant diagraph" as consonant_diagraph_end,
  e."Magic E words" as magic_E_words_end,
  e."Vowel diagraphs" as vowel_diagraphs_end,
  e."Multi syllabelle words" as multi_syllabelle_words_end,
  e."Passage 1" as passage_1_end,
  e."Passage 2" as passage_2_end,
  e."RF %" as RF_perc_end,
  e."Developing" as developing_end,
  e."Beginner" as beginner_end,
  e."Intermediate" as intermediate_end,
  e."Advanced" as advanced_end,
  e."RC Assessed %." as RC_assessed_perc_end,
  e."Endline RF Code" as RF_code_end
from endline e
where e."Student ID" <> ''