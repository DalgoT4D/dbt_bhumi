WITH unified_volunteers AS (

  -- 1. Corporate Volunteering
  SELECT
    'corporate'                                        AS volunteer_type,
    location                                           AS location,
    corporate_name,
    CASE WHEN no_of_volunteers IN ('', '-') THEN NULL ELSE no_of_volunteers::INT END AS no_of_volunteers,
    volunteering_month,
    CASE WHEN hours_per_volunteer IN ('', '-') THEN NULL ELSE hours_per_volunteer::FLOAT END AS hours_per_volunteer,
    CASE WHEN total_volunteering_hours_d_x_e_ IN ('', '-') THEN NULL ELSE total_volunteering_hours_d_x_e_::FLOAT END AS total_volunteering_hours,
    brief_description_about_the_activity               AS activity_description,
    -- Intern-only columns
    NULL::VARCHAR  AS category,
    NULL::VARCHAR  AS name_of_volunteer,
    NULL::VARCHAR  AS institution_name,
    NULL::VARCHAR  AS education_background,
    NULL::VARCHAR  AS email_id,
    NULL::VARCHAR  AS phone_number,
    NULL::VARCHAR  AS cluster_mapped,
    NULL::VARCHAR  AS manager_mapped,
    NULL::VARCHAR  AS certificate_issued,
    NULL::VARCHAR  AS start_date,
    NULL::VARCHAR  AS end_date,
    _airbyte_raw_id,
    _airbyte_extracted_at
  from {{ source('Stem_gsheet_data', 'Corporate_Volunteering_Data') }}

  UNION ALL

  -- 2. STEM Fest Volunteers
  SELECT
    'stem_fest'                                        AS volunteer_type,
    stem_fest_location                                 AS location,
    NULL::VARCHAR                                      AS corporate_name,
    CASE WHEN no_of_volunteers IN ('', '-') THEN NULL ELSE no_of_volunteers::INT END AS no_of_volunteers,
    volunteering_month,
    CASE WHEN hours_per_volunteer IN ('', '-') THEN NULL ELSE hours_per_volunteer::FLOAT END AS hours_per_volunteer,
    CASE WHEN total_volunteering_hours_cx_d_ IN ('', '-') THEN NULL ELSE total_volunteering_hours_cx_d_::FLOAT END AS total_volunteering_hours,
    NULL::VARCHAR                                      AS activity_description,
    NULL::VARCHAR  AS category,
    NULL::VARCHAR  AS name_of_volunteer,
    NULL::VARCHAR  AS institution_name,
    NULL::VARCHAR  AS education_background,
    NULL::VARCHAR  AS email_id,
    NULL::VARCHAR  AS phone_number,
    NULL::VARCHAR  AS cluster_mapped,
    NULL::VARCHAR  AS manager_mapped,
    NULL::VARCHAR  AS certificate_issued,
    NULL::VARCHAR  AS start_date,
    NULL::VARCHAR  AS end_date,
    _airbyte_raw_id,
    _airbyte_extracted_at
    FROM {{ source('Stem_gsheet_data', 'STEM_FEST_X_Volunteers') }}

  UNION ALL

  -- 3. Interns / Volunteers
  SELECT
    'intern'                                           AS volunteer_type,
    cluster_mapped                                     AS location,
    NULL::VARCHAR                                      AS corporate_name,
    NULL::INT                                          AS no_of_volunteers,
    intern_volunteer_month                             AS volunteering_month,
    NULL::FLOAT                                        AS hours_per_volunteer,
    CASE WHEN total_intern_hours IN ('', '-') THEN NULL ELSE total_intern_hours::FLOAT END AS total_volunteering_hours,
    activities_engaged_in                              AS activity_description,
    category,
    name_of_the_intern_volunteer                       AS name_of_volunteer,
    name_institution_name_                             AS institution_name,
    education_background_course_enrlolled_in           AS education_background,
    email_id_                                          AS email_id,
    phone_number,
    cluster_mapped,
    manager_mapped,
    certificate_issued,
    start_date,
    end_date,
    _airbyte_raw_id,
    _airbyte_extracted_at
  from {{ source('Stem_gsheet_data', 'Interns_Volunteer_Tracker') }}


)
SELECT * FROM unified_volunteers