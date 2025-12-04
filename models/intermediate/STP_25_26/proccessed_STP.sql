WITH normalized_students AS (
    SELECT
        s.*,
        lower(trim(regexp_replace(regexp_replace(s."School Name", '\.+$', '', 'g'), '\s+', ' ', 'g'))) AS norm_school,
        lower(trim(regexp_replace(regexp_replace(s."Student Name", '\.+$', '', 'g'), '\s+', ' ', 'g'))) AS norm_name
    FROM {{ ref('All_Students_STP') }} s
),

session_students AS (
    SELECT
        sa.school_name,
        sa.trainer_name,
        sa."class"            AS class,
        sa.class_division,
        sa.class_type,
        sa.session_type,
        sa.intervention_name,
        sa.session_date::date AS session_date,
        trim(ss_raw.student_name_raw) AS student_name,
        ss_raw.status                 AS status,
        lower(trim(regexp_replace(regexp_replace(sa.school_name, '\.+$', '', 'g'), '\s+', ' ', 'g'))) AS norm_school,
        lower(trim(regexp_replace(regexp_replace(ss_raw.student_name_raw, '\.+$', '', 'g'), '\s+', ' ', 'g'))) AS norm_name
    FROM {{ ref('session_attendance_STP') }} sa
        CROSS JOIN LATERAL (
         SELECT unnest(coalesce(sa.present_students_list_arr, ARRAY[]::text[])) AS student_name_raw,
             'Present'::text AS status
         UNION ALL
         SELECT unnest(coalesce(sa.absent_students_list_arr, ARRAY[]::text[])) AS student_name_raw,
             'Absent'::text AS status
        ) ss_raw
    WHERE trim(ss_raw.student_name_raw) <> ''
)

SELECT
    ss.school_name       AS "School Name",
    ss.trainer_name      AS "Trainer",
    ss.class             AS "Class",
    ss.class_division    AS "Class Division",
    ss.intervention_name AS "Intervention Name",
    ss.class_type        AS "Class Type",
    ss.session_type      AS "Session Type",
    ss.session_date      AS "Session Date",
    ss.student_name      AS "Student Name",
    ss.status            AS "Status",
    coalesce(st.donor,   '') AS "Donor",
    coalesce(st.district,'') AS "District",
    coalesce(st.state,   '') AS "State",
    coalesce(st.gender,  '') AS "Gender"
FROM session_students ss
LEFT JOIN normalized_students st
       ON ss.norm_school = st.norm_school
      AND ss.norm_name  = st.norm_name
ORDER BY
    ss.school_name,
    ss.class,
    ss.class_division,
    ss.session_date,
    ss.student_name
