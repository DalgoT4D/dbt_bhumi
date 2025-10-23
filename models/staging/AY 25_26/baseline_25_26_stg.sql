WITH baseline AS (
    SELECT
        COALESCE(BTRIM("Acadamic_year"::TEXT), '') AS "Acadamic year",
        COALESCE(INITCAP(BTRIM("City"::TEXT)), '') AS "City",
        COALESCE(INITCAP(BTRIM("PM_Name"::TEXT)), '') AS "PM Name",
        COALESCE(INITCAP(BTRIM("School_Name"::TEXT)), '') AS "School Name",
        COALESCE(INITCAP(BTRIM("Classroom_ID"::TEXT)), '') AS "Classroom ID",
        COALESCE(INITCAP(BTRIM("Fellow_name"::TEXT)), '') AS "Fellow name",
        CASE WHEN BTRIM("Cohort"::TEXT) ~ '^\d+$' THEN ("Cohort"::TEXT)::INTEGER ELSE NULL END AS "Cohort",
        CASE WHEN BTRIM("Student_grade"::TEXT) ~ '^\d+$' THEN ("Student_grade"::TEXT)::INTEGER ELSE NULL END AS "Student grade",
        COALESCE(BTRIM("Student_ID"::TEXT), '') AS "Student ID",
        COALESCE(INITCAP(BTRIM("Student_name"::TEXT)), '') AS "Student name",

        -- RC Baseline
        COALESCE(BTRIM("RC_level_Baseline"::TEXT), '') AS "RC level Baseline",
        COALESCE(BTRIM("RC_Grade_level_Baseline"::TEXT), '') AS "RC Grade level Baseline",
        COALESCE(BTRIM("RC_Learning_Level_Status_Baseline"::TEXT), '') AS "RC Learning Level Status Baseline",
        NULLIF(BTRIM("RC_Endline_Baseline_Growth"::TEXT), '') AS "RC Endline Baseline Growth",

        -- RC Baseline Scores
        CASE WHEN BTRIM("Baseline_Factual"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Baseline_Factual", '%','')::NUMERIC ELSE NULL END AS "Baseline Factual",
        CASE WHEN BTRIM("Baseline_Inference"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Baseline_Inference", '%','')::NUMERIC ELSE NULL END AS "Baseline Inference",
        CASE WHEN BTRIM("Baseline_Critical_Thinking"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Baseline_Critical_Thinking", '%','')::NUMERIC ELSE NULL END AS "Baseline Critical Thinking",
        CASE WHEN BTRIM("Baseline_Vocabulary"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Baseline_Vocabulary", '%','')::NUMERIC ELSE NULL END AS "Baseline Vocabulary",
        CASE WHEN BTRIM("Baseline_Grammar"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Baseline_Grammar", '%','')::NUMERIC ELSE NULL END AS "Baseline Grammar",
        CASE WHEN BTRIM("Baseline_Assessed_Percentage"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Baseline_Assessed_Percentage", '%','')::NUMERIC ELSE NULL END AS "Baseline Assessed Percentage",

        -- Math Baseline
        COALESCE(BTRIM("Math_Level_Baseline_"::TEXT), '') AS "Math Level Baseline",
        CASE WHEN BTRIM("Final_Baseline_Level_Mastery"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Final_Baseline_Level_Mastery", '%','')::NUMERIC ELSE NULL END AS "Final Baseline Level Mastery",
        CASE WHEN BTRIM("Math_Baseline_Grade"::TEXT) ~ '^\d+$' THEN ("Math_Baseline_Grade"::TEXT)::INTEGER ELSE NULL END AS "Math Baseline Grade",
        COALESCE(BTRIM("Math_Learning_level_status_Baseline"::TEXT), '') AS "Math Learning Level Status Baseline",
        NULLIF(BTRIM("Math_Endline_Baseline_Growth"::TEXT), '') AS "Math Endline Baseline Growth",
        CASE WHEN BTRIM("Baseline_Numbers"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Baseline_Numbers", '%','')::NUMERIC ELSE NULL END AS "Baseline Numbers",
        CASE WHEN BTRIM("Baseline_Patterns"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Baseline_Patterns", '%','')::NUMERIC ELSE NULL END AS "Baseline Patterns",
        CASE WHEN BTRIM("Baseline_Geometry"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Baseline_Geometry", '%','')::NUMERIC ELSE NULL END AS "Baseline Geometry",
        CASE WHEN BTRIM("Baseline_Total_in_Mensuration"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Baseline_Total_in_Mensuration", '%','')::NUMERIC ELSE NULL END AS "Baseline Total in Mensuration",
        CASE WHEN BTRIM("Baseline_Total_in_Time"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Baseline_Total_in_Time", '%','')::NUMERIC ELSE NULL END AS "Baseline Total in Time",
        CASE WHEN BTRIM("Baseline_Total_in_Operations"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Baseline_Total_in_Operations", '%','')::NUMERIC ELSE NULL END AS "Baseline Total in Operations",
        CASE WHEN BTRIM("Baseline_Total_in_Data"::TEXT) ~ '^[0-9.]+%?$' THEN REPLACE("Baseline_Total_in_Data", '%','')::NUMERIC ELSE NULL END AS "Baseline Total in Data",

        -- RF Baseline
        COALESCE(BTRIM("RF_Level_Baseline"::TEXT), '') AS "RF Level Baseline",
        NULLIF(BTRIM("RF_Baseline_Growth"::TEXT), '') AS "RF Baseline Growth",
        CASE WHEN BTRIM("Baseline_Letter_sounds"::TEXT) ~ '^[0-9.]+$' THEN ("Baseline_Letter_sounds"::TEXT)::NUMERIC ELSE NULL END AS "Baseline Letter sounds",
        CASE WHEN BTRIM("Baseline_CVC_words"::TEXT) ~ '^[0-9.]+$' THEN ("Baseline_CVC_words"::TEXT)::NUMERIC ELSE NULL END AS "Baseline CVC words",
        CASE WHEN BTRIM("Baseline_Blends"::TEXT) ~ '^[0-9.]+$' THEN ("Baseline_Blends"::TEXT)::NUMERIC ELSE NULL END AS "Baseline Blends",
        CASE WHEN BTRIM("Baseline_Consonant_diagraph"::TEXT) ~ '^[0-9.]+$' THEN ("Baseline_Consonant_diagraph"::TEXT)::NUMERIC ELSE NULL END AS "Baseline Consonant diagraph",
        CASE WHEN BTRIM("Baseline_Magic_E_words"::TEXT) ~ '^[0-9.]+$' THEN ("Baseline_Magic_E_words"::TEXT)::NUMERIC ELSE NULL END AS "Baseline Magic E words",
        CASE WHEN BTRIM("Baseline_Vowel_diagraphs"::TEXT) ~ '^[0-9.]+$' THEN ("Baseline_Vowel_diagraphs"::TEXT)::NUMERIC ELSE NULL END AS "Baseline Vowel diagraphs",
        CASE WHEN BTRIM("Baseline_Multi_syllabelle_words"::TEXT) ~ '^[0-9.]+$' THEN ("Baseline_Multi_syllabelle_words"::TEXT)::NUMERIC ELSE NULL END AS "Baseline Multi syllabelle words",
        CASE WHEN BTRIM("Baseline_Passage_1"::TEXT) ~ '^[0-9.]+$' THEN ("Baseline_Passage_1"::TEXT)::NUMERIC ELSE NULL END AS "Baseline Passage 1",
        CASE WHEN BTRIM("Baseline_Passage_2"::TEXT) ~ '^[0-9.]+$' THEN ("Baseline_Passage_2"::TEXT)::NUMERIC ELSE NULL END AS "Baseline Passage 2"

    FROM {{ source('fellowship_25_26', 'Raw_Data_Baseline_2025') }}
)

SELECT DISTINCT
    b."Acadamic year" AS "Acadamic year",
    b."City" AS "City",
    b."PM Name" AS "PM Name",
    b."School Name" AS "School Name",
    b."Classroom ID" AS "Classroom ID",
    b."Fellow name" AS "Fellow name",
    b."Cohort" AS "Cohort",
    b."Student grade" AS "Student grade",
    b."Student ID" AS "Student ID",
    b."Student name" AS "Student name",
    b."RC level Baseline" AS "RC level Baseline",
    b."RC Grade level Baseline" AS "RC Grade level Baseline",
    b."RC Learning Level Status Baseline" AS "RC Learning Level Status Baseline",
    b."RC Endline Baseline Growth" AS "RC Endline Baseline Growth",
    b."Baseline Factual" AS "Baseline Factual",
    b."Baseline Inference" AS "Baseline Inference",
    b."Baseline Critical Thinking" AS "Baseline Critical Thinking",
    b."Baseline Vocabulary" AS "Baseline Vocabulary",
    b."Baseline Grammar" AS "Baseline Grammar",
    b."Baseline Assessed Percentage" AS "Baseline Assessed Percentage",
    b."Math Level Baseline" AS "Math Level Baseline",
    b."Final Baseline Level Mastery" AS "Final Baseline Level Mastery",
    b."Math Baseline Grade" AS "Math Baseline Grade",
    b."Math Learning Level Status Baseline" AS "Math Learning Level Status Baseline",
    b."Math Endline Baseline Growth" AS "Math Endline Baseline Growth",
    b."Baseline Numbers" AS "Baseline Numbers",
    b."Baseline Patterns" AS "Baseline Patterns",
    b."Baseline Geometry" AS "Baseline Geometry",
    b."Baseline Total in Mensuration" AS "Baseline Total in Mensuration",
    b."Baseline Total in Time" AS "Baseline Total in Time",
    b."Baseline Total in Operations" AS "Baseline Total in Operations",
    b."Baseline Total in Data" AS "Baseline Total in Data"
FROM baseline b
WHERE b."Student ID" <> ''


