with teacher_champion_scoring as (
    select
        -- identifiers
        case when Btrim(s_no::text) ~ '^\d+$' then (s_no::text)::integer end as s_no,
        Coalesce(Initcap(Btrim(donor::text)), '') as donor,
        Coalesce(Initcap(Btrim(level::text)), '') as level,
        Coalesce(Initcap(Btrim(district::text)), '') as district,
        Coalesce(Btrim(grades_covered::text), '') as grades_covered,
        Coalesce(Btrim(scoring_period_::text), '') as scoring_period,
        Coalesce(Btrim(subject_handled::text), '') as subject_handled,
        Coalesce(Initcap(Btrim(name_of_the_school::text)), '') as school_name,
        Coalesce(Initcap(Btrim(name_of_the_teacher::text)), '') as teacher_name,
        Coalesce(Btrim(teacher_identifies_as::text), '') as teacher_identifies_as,

        -- numeric conversions
        case when Btrim(total_mark::text) ~ '^[0-9]+(\.[0-9]+)?$' then (total_mark::text)::numeric end as total_mark,
        case when Btrim(_1_facilitation_lesson_integration::text) ~ '^\d+$' then (_1_facilitation_lesson_integration::text)::integer end as facilitation_lesson_integration,
        case when Btrim(_2_student_engagement_mentorship::text) ~ '^\d+$' then (_2_student_engagement_mentorship::text)::integer end as student_engagement_mentorship,
        case when Btrim(_3_resource_lab_ownership::text) ~ '^\d+$' then (_3_resource_lab_ownership::text)::integer end as resource_lab_ownership,
        case when Btrim(_4_innovation_contextualization::text) ~ '^\d+$' then (_4_innovation_contextualization::text)::integer end as innovation_contextualization,
        case when Btrim(_5_training_participation_knowledge_sharing::text) ~ '^\d+$' then (_5_training_participation_knowledge_sharing::text)::integer end as training_participation_knowledge_sharing,
        case when Btrim(_6_leadership_initiative::text) ~ '^\d+$' then (_6_leadership_initiative::text)::integer end as leadership_initiative,
        case when Btrim(_7_documentation_sustainability_mindset::text) ~ '^\d+$' then (_7_documentation_sustainability_mindset::text)::integer end as documentation_sustainability_mindset
    from {{ source('Stem_gsheet_data', 'Teacher_Champion_Scoring') }}
)

select distinct
    s_no,
    donor,
    level,
    district,
    school_name,
    teacher_name,
    teacher_identifies_as,
    grades_covered,
    scoring_period,
    subject_handled,
    total_mark,
    facilitation_lesson_integration,
    student_engagement_mentorship,
    resource_lab_ownership,
    innovation_contextualization,
    training_participation_knowledge_sharing,
    leadership_initiative,
    documentation_sustainability_mindset
from teacher_champion_scoring
where school_name != ''
