with baseline as (
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
    Coalesce(btrim("Baseline_RC_Level"::text), '') as "Baseline RC Level",
    Coalesce(btrim("Basel_RC_Grade_Level"::text), '') as "Baseline RC Grade Level",
    Coalesce(initcap(btrim("Baseline_RC_Status"::text)), '') as "Baseline RC Status",
    Coalesce(initcap(btrim("Baseline_RF_Status"::text)), '') as "Baseline RF Status",
    Coalesce(initcap(btrim("Baseline_Math_Level"::text)), '') as "Baseline Math Level",
    Coalesce(initcap(btrim("Baseline_Math_Status"::text)), '') as "Baseline Math Status",
    case when lower(btrim("Baseline_Math_Mastery"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Baseline_Math_Mastery"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Baseline_Math_Mastery"::text), '%','')::numeric
         when btrim("Baseline_Math_Mastery"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math_Mastery"::text)::numeric
         else null end as "Baseline Math Mastery",
    case when lower(btrim("Baseline_Math___in_Numbers"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Baseline_Math___in_Numbers"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Baseline_Math___in_Numbers"::text), '%','')::numeric
         when btrim("Baseline_Math___in_Numbers"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Numbers"::text)::numeric
         else null end as "Baseline Math % in Numbers",
    case when lower(btrim("Baseline_Math___in_Patterns"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Baseline_Math___in_Patterns"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Baseline_Math___in_Patterns"::text), '%','')::numeric
         when btrim("Baseline_Math___in_Patterns"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Patterns"::text)::numeric
         else null end as "Baseline Math % in Patterns",
    case when lower(btrim("Baseline_Math___in_Geometry"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Baseline_Math___in_Geometry"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Baseline_Math___in_Geometry"::text), '%','')::numeric
         when btrim("Baseline_Math___in_Geometry"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Geometry"::text)::numeric
         else null end as "Baseline Math % in Geometry",
    case when lower(btrim("Baseline_Math___in_Mensuration"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Baseline_Math___in_Mensuration"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Baseline_Math___in_Mensuration"::text), '%','')::numeric
         when btrim("Baseline_Math___in_Mensuration"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Mensuration"::text)::numeric
         else null end as "Baseline Math % in Mensuration",
    case when lower(btrim("Baseline_Math___in_Time"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Baseline_Math___in_Time"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Baseline_Math___in_Time"::text), '%','')::numeric
         when btrim("Baseline_Math___in_Time"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Time"::text)::numeric
         else null end as "Baseline Math % in Time",
    case when lower(btrim("Baseline_Math___in_Operations"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Baseline_Math___in_Operations"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Baseline_Math___in_Operations"::text), '%','')::numeric
         when btrim("Baseline_Math___in_Operations"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Operations"::text)::numeric
         else null end as "Baseline Math % in Operations",
    case when lower(btrim("Baseline_Math___in_Data_Handling"::text)) in ('', 'na', 'not assessed', 'not assessed.') then null
         when btrim("Baseline_Math___in_Data_Handling"::text) ~ '^[0-9]+(\.[0-9]+)?%$' then replace(btrim("Baseline_Math___in_Data_Handling"::text), '%','')::numeric
         when btrim("Baseline_Math___in_Data_Handling"::text) ~ '^[0-9]+(\.[0-9]+)?$' then ("Baseline_Math___in_Data_Handling"::text)::numeric
         else null end as "Baseline Math % in Data Handling",
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
    case when btrim("Baseline_RF_Code"::text) ~ '^\d+$' then ("Baseline_RF_Code"::text)::integer else null end as "Baseline RF Code"
  from {{ source('fellowship_24_25_data', 'Raw_Data_Baseline') }}
)

select distinct
  b."Student ID" as student_id_base,
  b."City" as city_base,
  b."PM Name" as PM_name_base,
  b."School Name" as school_name_base,
  b."Classroom ID" as classroom_id_base,
  b."Fellow Name" as fellow_name_base,
  b."Cohort" as cohort_base,
  b."Grade Taught" as grade_taught_base,
  b."Student Name" as student_name_base,
  b."Baseline RC Level" as RC_level_base,
  b."Baseline RC Grade Level" as RC_grade_level_base, 
  b."Baseline RC Status" as RC_status_base,
  b."Baseline RF Status" as RF_status_base,
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
  b."CVC words" as CVC_words_base,
  b."Blends" as blends_base,
  b."Consonant diagraph" as consonant_diagraph_base,
  b."Magic E words" as magic_E_words_base,
  b."Vowel diagraphs" as vowel_diagraphs_base,
  b."Multi syllabelle words" as multi_syllabelle_words_base,
  b."Passage 1" as passage_1_base,
  b."Passage 2" as passage_2_base,
  b."RF %" as RF_perc_base,
  b."Developing" as developing_base,
  b."Beginner" as beginner_base,
  b."Intermediate" as intermediate_base,
  b."Advanced" as advanced_base,
  b."RC Assessed %." as RC_assessed_perc_base,
  b."Baseline RF Code" as RF_code_base
from baseline b
where b."Student ID" <> ''


