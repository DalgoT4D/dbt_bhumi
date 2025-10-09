select distinct
  nullif(trim("Grade"), '') as "Grade",
  nullif(trim("School"), '') as "School",
  nullif(trim("Chapter"), '') as "Chapter",
  nullif(trim("Climate"), '') as "Climate",
  nullif(trim("Endline"), '') as "Endline",
  nullif(trim("Baseline"), '') as "Baseline",
  nullif(trim("Roll_No_"), '') as "Roll_No_",
  nullif(trim("School_ID"), '') as "School_ID",
  nullif(trim("Attendance__"), '') as "Attendance__",
  nullif(trim("EndlineScore"), '') as "EndlineScore",
  nullif(trim("_Student_Name_"), '') as "_Student_Name_",
  nullif(trim("Baseline_Score"), '') as "Baseline_Score",
  nullif(trim("Kitchen_Garden"), '') as "Kitchen_Garden",
  nullif(trim("Student_Status"), '') as "Student_Status",
  nullif(trim("BaselineScore_"), '') as "BaselineScore_",
  nullif(trim("Waste_Management"), '') as "Waste_Management",
  nullif(trim("Modlues_Completed"), '') as "Modlues_Completed",
  nullif(trim("Water_Conservation"), '') as "Water_Conservation",
  nullif(trim("Life_Style___Choices"), '') as "Life_Style___Choices",
  nullif(trim("Center_Coordiantor__1_"), '') as "Center_Coordiantor__1_",
  nullif(trim("Center_Coordianator__2_"), '') as "Center_Coordianator__2_"

from {{ select
  nullif(trim("School"), '') as "School",                                 -- string
  nullif(trim("School ID"), '') as "School_ID",                          -- string
  nullif(trim("Grade"), '') as "Grade",                                  -- string
  cast(nullif(trim("Student Count"), '') as integer) as "Student_Count", -- integer
  nullif(trim("Center Coordiantor (1)"), '') as "Center_Coordiantor_1",  -- string
  nullif(trim("Center Coordianator (2)"), '') as "Center_Coordianator_2",-- string
  cast(nullif(trim("Baseline"), '') as numeric) as "Baseline",           -- numeric/float
  nullif(trim("Kitchen Garden"), '') as "Kitchen_Garden",                -- string
  nullif(trim("Waste Management"), '') as "Waste_Management",            -- string
  nullif(trim("Water Conservation"), '') as "Water_Conservation",        -- string
  nullif(trim("Climate"), '') as "Climate",                              -- string
  nullif(trim("Lifestyle & Choices"), '') as "Lifestyle_Choices",        -- string
  cast(nullif(trim("Endline"), '') as numeric) as "Endline",             -- numeric/float
  nullif(trim("Mo"), '') as "Mo",                                        -- string
  nullif(trim("Session Completion Status"), '') as "Session_Completion_Status", -- string
  cast(nullif(trim("Classroom Attendance"), '') as integer) as "Classroom_Attendance", -- integer
  cast(nullif(trim("Progress Bar (Q2) (Target = 2 Modules)"), '') as numeric) as "Progress_Bar_Q2", -- numeric/float
  cast(nullif(trim("Total Students Count"), '') as integer) as "Total_Students_Count", -- integer
  nullif(trim("Donor Mapped"), '') as "Donor_Mapped"                     -- string
from {{ source('ecochamps25_26', 'Data_Tracker') }} }}
