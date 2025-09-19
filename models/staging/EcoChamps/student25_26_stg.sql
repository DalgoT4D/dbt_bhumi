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

from {{ source('ecochamps25_26', 'Student___Session_Data') }}
