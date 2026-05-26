-- pull thresholds for each RAG parameter
with rag_thresholds as (
    select
        parameter,
        black_percent_comp,
        red_percent_comp,
        amber_percent_comp,
        green_percent_comp,
        gold_percent_comp
    from {{ ref('stem_rag_params_stg') }}
),

-- ── KPI 1: Planned classes conducted ─────────────────────────────────────────
school_sessions as (
    select
        academic_year,
        academic_quarter,
        trainer_name,
        school_name,
        sum(sessions_planned) as total_planned,
        sum(sessions_conducted) as total_conducted
    from {{ ref('stem_session_plan_conduct_prd') }}
    where sessions_planned > 0
    group by
        academic_year,
        academic_quarter,
        trainer_name,
        school_name
),

school_sessions_pct as (
    select
        academic_year,
        academic_quarter,
        trainer_name,
        school_name,
        total_planned,
        total_conducted,
        round(total_conducted::numeric / nullif(total_planned, 0), 2) as conducted_pct
    from school_sessions
),

school_sessions_rag as (
    select
        sp.academic_year,
        sp.academic_quarter,
        sp.trainer_name,
        sp.school_name,
        sp.conducted_pct,
        case
            when sp.conducted_pct <= r.black_percent_comp then 'Black'
            when sp.conducted_pct <= r.red_percent_comp then 'Red'
            when sp.conducted_pct <= r.amber_percent_comp then 'Amber'
            when sp.conducted_pct <= r.green_percent_comp then 'Green'
            else 'Gold'
        end as sessions_rag_category
    from school_sessions_pct as sp
    join rag_thresholds as r
        on r.parameter = 'Planned classes conducted'
),

-- ── KPI 2: Lesson Plans with Experiment/Model Integration ────────────────────
school_experiment as (
    select
        academic_year,
        academic_quarter,
        trainer_name,
        school_name,
        sum(session_count) as total_sessions,
        sum(
            case
                when lower(session_type) in ('experimental', 'both') then session_count
                else 0
            end
        ) as experiment_sessions
    from {{ ref('stem_session_type_prd') }}
    group by
        academic_year,
        academic_quarter,
        trainer_name,
        school_name
),

school_experiment_pct as (
    select
        academic_year,
        academic_quarter,
        trainer_name,
        school_name,
        total_sessions,
        experiment_sessions,
        round(experiment_sessions::numeric / nullif(total_sessions, 0), 2) as experiment_pct
    from school_experiment
),

school_experiment_rag as (
    select
        sp.academic_year,
        sp.academic_quarter,
        sp.trainer_name,
        sp.school_name,
        sp.experiment_pct,
        case
            when sp.experiment_pct <= r.black_percent_comp then 'Black'
            when sp.experiment_pct <= r.red_percent_comp then 'Red'
            when sp.experiment_pct <= r.amber_percent_comp then 'Amber'
            when sp.experiment_pct <= r.green_percent_comp then 'Green'
            else 'Gold'
        end as experiment_rag_category
    from school_experiment_pct as sp
    join rag_thresholds as r
        on r.parameter = 'Lesson Plans with Experiment/model Integration'
),

-- ── KPI 3: Teacher Led STEM Classes ──────────────────────────────────────────
school_teacher_classes as (
    select
        academic_year,
        academic_quarter,
        trainer_name,
        school_name,
        sum(no_of_classes) as actual_classes,
        sum(no_of_teachers) as total_teachers
    from {{ ref('stem_teacher_led_classes_prd') }}
    group by
        academic_year,
        academic_quarter,
        trainer_name,
        school_name
),

school_teacher_classes_pct as (
    select
        academic_year,
        academic_quarter,
        trainer_name,
        school_name,
        actual_classes,
        total_teachers * 2 as expected_classes,
        --expected classes = 2 classes/teacher every month × 3 months/quarter
        round(actual_classes::numeric / nullif(total_teachers * 6, 0), 2) as teacher_class_pct
    from school_teacher_classes
),

school_teacher_classes_rag as (
    select
        sp.academic_year,
        sp.academic_quarter,
        sp.trainer_name,
        sp.school_name,
        sp.teacher_class_pct,
        case
            when sp.teacher_class_pct <= r.black_percent_comp then 'Black'
            when sp.teacher_class_pct <= r.red_percent_comp then 'Red'
            when sp.teacher_class_pct <= r.amber_percent_comp then 'Amber'
            when sp.teacher_class_pct <= r.green_percent_comp then 'Green'
            else 'Gold'
        end as teacher_class_rag_category
    from school_teacher_classes_pct as sp
    join rag_thresholds as r
        on r.parameter = 'Teacher led STEM Classes'
),

-- ── KPI 4: % of Teachers at Level 3+ on Teacher Champion Rubrics ─────────────
-- note: school-level only, no trainer dimension in source
school_teacher_champion as (
    select
        academic_year,
        academic_quarter,
        school_name,
        sum(total_teachers) as total_teachers,
        sum(teachers_level_3_plus) as total_level_3_plus
    from {{ ref('stem_teacher_champion_scoring_prd') }}
    group by
        academic_year,
        academic_quarter,
        school_name
),

school_teacher_champion_pct as (
    select
        academic_year,
        academic_quarter,
        school_name,
        total_teachers,
        total_level_3_plus,
        round(total_level_3_plus::numeric / nullif(total_teachers, 0), 2) as teacher_champion_pct
    from school_teacher_champion
),

school_teacher_champion_rag as (
    select
        sp.academic_year,
        sp.academic_quarter,
        sp.school_name,
        sp.teacher_champion_pct,
        case
            when sp.teacher_champion_pct <= r.black_percent_comp then 'Black'
            when sp.teacher_champion_pct <= r.red_percent_comp then 'Red'
            when sp.teacher_champion_pct <= r.amber_percent_comp then 'Amber'
            when sp.teacher_champion_pct <= r.green_percent_comp then 'Green'
            else 'Gold'
        end as teacher_champion_rag_category
    from school_teacher_champion_pct as sp
    join rag_thresholds as r
        on r.parameter = '% of Teachers in Level.3 on Teacher champion rubrics'
),

-- ── KPI 5: Trainer TCM Score on Rubrics (unit: trainer) ──────────────────────
-- note: no school dimension; joins on trainer_name
trainer_tcm as (
    select
        academic_year,
        academic_quarter,
        trainer as trainer_name,
        trainer_competency_level
    from {{ ref('stem_trainer_tcm_prd') }}
    where trainer_competency_level is not null
),

trainer_tcm_rag as (
    select
        tr.academic_year,
        tr.academic_quarter,
        tr.trainer_name,
        tr.trainer_competency_level,
        case
            when tr.trainer_competency_level <= r.black_percent_comp then 'Black'
            when tr.trainer_competency_level <= r.red_percent_comp then 'Red'
            when tr.trainer_competency_level <= r.amber_percent_comp then 'Amber'
            when tr.trainer_competency_level <= r.green_percent_comp then 'Green'
            else 'Gold'
        end as trainer_tcm_rag_category
    from trainer_tcm as tr
    join rag_thresholds as r
        on r.parameter = 'No of Trainers in Level.3 on Rubrics'
),

-- ── KPI 6: Active STEM Clubs ──────────────────────────────────────────────────
school_clubs as (
    select
        academic_year,
        academic_quarter,
        trainer,
        school_name,
        sum(total_activities) as actual_activities,
        -- expected = 2 activities/month × 3 months/quarter
        6 as expected_activities
    from {{ ref('stem_clubs_prd') }}
    group by
        academic_year,
        academic_quarter,
        trainer,
        school_name
),

school_clubs_pct as (
    select
        academic_year,
        academic_quarter,
        trainer,
        school_name,
        actual_activities,
        expected_activities,
        round(actual_activities::numeric / nullif(expected_activities, 0), 2) as club_activity_pct
    from school_clubs
),

school_clubs_rag as (
    select
        sp.academic_year,
        sp.academic_quarter,
        sp.trainer as trainer_name,
        sp.school_name,
        sp.club_activity_pct,
        case
            when sp.club_activity_pct <= r.black_percent_comp then 'Black'
            when sp.club_activity_pct <= r.red_percent_comp then 'Red'
            when sp.club_activity_pct <= r.amber_percent_comp then 'Amber'
            when sp.club_activity_pct <= r.green_percent_comp then 'Green'
            else 'Gold'
        end as club_activity_rag_category
    from school_clubs_pct as sp
    join rag_thresholds as r
        on r.parameter = 'Active STEM Clubs'
)

select
    academic_year,
    academic_quarter,
    initcap(lower(trim(school_name))) as school_name,
    initcap(lower(trim(trainer_name))) as trainer_name,
    'Planned classes conducted' as kpi_name,
    conducted_pct as pct_value,
    sessions_rag_category as rag_category
from school_sessions_rag

union all

select
    academic_year,
    academic_quarter,
    initcap(lower(trim(school_name))) as school_name,
    initcap(lower(trim(trainer_name))) as trainer_name,
    'Lesson Plans with Experiment/model Integration' as kpi_name,
    experiment_pct as pct_value,
    experiment_rag_category as rag_category
from school_experiment_rag

union all

select
    academic_year,
    academic_quarter,
    initcap(lower(trim(school_name))) as school_name,
    initcap(lower(trim(trainer_name))) as trainer_name,
    'Teacher led STEM Classes' as kpi_name,
    teacher_class_pct as pct_value,
    teacher_class_rag_category as rag_category
from school_teacher_classes_rag

union all

select
    academic_year,
    academic_quarter,
    initcap(lower(trim(school_name))) as school_name,
    null as trainer_name,
    '% of Teachers in Level 3 on Teacher Champion Rubrics' as kpi_name,
    teacher_champion_pct as pct_value,
    teacher_champion_rag_category as rag_category
from school_teacher_champion_rag

union all

select
    academic_year,
    academic_quarter,
    null as school_name,
    initcap(lower(trim(trainer_name))) as trainer_name,
    'No of Trainers in Level 3 on Rubrics' as kpi_name,
    trainer_competency_level as pct_value,
    trainer_tcm_rag_category as rag_category
from trainer_tcm_rag

union all

select
    academic_year,
    academic_quarter,
    initcap(lower(trim(school_name))) as school_name,
    initcap(lower(trim(trainer_name))) as trainer_name,
    'Active STEM Clubs' as kpi_name,
    club_activity_pct as pct_value,
    club_activity_rag_category as rag_category
from school_clubs_rag

order by
    kpi_name,
    academic_year,
    academic_quarter,
    school_name,
    trainer_name,
    kpi_name
