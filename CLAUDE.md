# CLAUDE.md — dbt_bhumi

This file provides guidance for AI assistants working in this repository.

---

## Project Overview

**dbt_bhumi** is a dbt data transformation project for **Bhumi**, an educational fellowship organization. It transforms raw assessment data from Google Sheets and a School App database into analytics-ready tables for dashboards and reporting.

**What it tracks:**
- Student learning outcomes across three assessment timepoints: Baseline, Midline, Endline
- Three subject areas: Reading Comprehension (RC), Reading Fluency (RF), Mathematics
- Multiple programs and academic years (see Programs section)

---

## Technology Stack

| Tool | Role |
|------|------|
| **dbt** (data build tool) | Core transformation framework |
| **PostgreSQL** | Target database |
| **Jinja2** | Templating inside SQL (via dbt) |
| **sqlfluff** | SQL linting (PostgreSQL dialect) |
| **dbt_utils** v1.1.1 | Utility macros |
| **dbt_expectations** v0.10.3 | Advanced data quality tests |
| **elementary** v0.15.2 | Data monitoring and observability |
| **dbt-osmosis** | Auto-manages schema.yml documentation |
| **pre-commit** | Git hooks for code quality |

**Python dependencies** (`requirements.txt`):
```
sqlfluff
dbt-postgres
sqlfluff-templater-dbt
pre_commit
dbt-osmosis
```

---

## Repository Structure

```
dbt_bhumi/
├── models/
│   ├── staging/                  # Layer 1: Raw data cleaning
│   │   ├── AY 24_25/             # AY 2024-25 staging models
│   │   ├── AY 25_26/             # AY 2025-26 staging models
│   │   ├── EcoChamps_25_26/      # EcoChamps staging models
│   │   ├── FSA_25_26/            # Fellowship School App staging
│   │   ├── schema.yml            # Staging model documentation
│   │   └── sources.yml           # Raw source table definitions
│   ├── intermediate/             # Layer 2: Aggregation and joining
│   │   ├── AY 24_25/
│   │   ├── AY 25_26/
│   │   ├── Ecochamps_25_26/
│   │   ├── FSA_25_26/
│   │   └── schema.yml
│   └── prod/                     # Layer 3: Analytics-ready outputs
│       ├── AY 24_25/
│       ├── AY 25_26/
│       ├── BF_SA_25_26/
│       ├── Ecochamp_25_26/
│       └── schema.yml
├── macros/                       # Reusable SQL macros
│   ├── validate_date.sql         # Multi-format date parser
│   ├── mask_pii.sql              # PII masking
│   └── generate_schema_name.sql  # Environment-aware schema naming
├── dbt_project.yml               # dbt configuration
├── packages.yml                  # dbt package dependencies
├── requirements.txt              # Python dependencies
├── .sqlfluff                     # SQL linting configuration
└── .pre-commit-config.yaml       # Pre-commit hooks
```

---

## Programs and Data Sources

### AY 24_25 (Academic Year 2024-25)
- **Source schema:** `fellowship_24_25`
- **Raw tables:** `Raw_Data_Baseline`, `Raw_Data_Midline`, `Raw_Data_Endline` (from Google Sheets)
- **Layers:** Staging (3 models), Intermediate (2 models), Prod (13 models)
- **Assessment timepoints:** Baseline, Midline, Endline (all three complete)
- **Key models:** `baseline_2425_stg`, `midline_2425_stg`, `endline_2425_stg`, `base_mid_end_comb_scores_2425_fct`, `base_mid_end_comb_students_2425_dim`

### AY 25_26 (Academic Year 2025-26)
- **Source schemas:** `fellowship_25_26`, `school_app`
- **Raw tables:** `Raw_Data_Baseline_2025`, `Raw_Data_Midline_2025`, plus school app tables (fellows, pms, schools, profiles, checkins, classroom_updates, observations)
- **Layers:** Staging (2 fellowship + 4 FSA models), Intermediate (2 fellowship + 3 FSA models), Prod (14 models)
- **Assessment timepoints:** Baseline, Midline only (Endline not yet available)
- **Difference from 24_25:** Adds `Donor` field, includes growth metric columns

### EcoChamps 25_26
- **Source schema:** `ecochamps25_26`
- **Raw tables:** `Student___Session_Data`, `Center_Coordiantors`, `Data_Tracker`, `RAG_Quarter_Params`
- **Layers:** Staging (3 models), Intermediate (2 models), Prod (6 models)
- **Focus:** Environmental education program — tracks 5 modules and attendance
- **Modules:** Kitchen Garden, Waste Management, Water Conservation, Climate, Lifestyle Choices
- **Extras:** RAG (Red-Amber-Green) quarterly monitoring, volunteer hours tracking

### FSA 25_26 (Fellowship School Associate)
- **Source schema:** `school_app`
- **Raw tables:** fellows, pms, fellow_school_grade, schools, profiles, checkins, classroom_updates, observations
- **Layers:** Staging (4 models), Intermediate (3 models), Prod (0 models — operational data)
- **Focus:** Operational/relational data — fellow/PM/school relationships, check-ins, classroom observations

### BF_SA 25_26
- **Source schema:** N/A (appears to be aggregated data)
- **Layers:** Prod only (2 models: `BRAG_sch_25_26`, `school_para_25_26`)
- **Focus:** School operations monitoring and BRAG status

---

## Assessment Subjects and Metrics

### Reading Comprehension (RC)
- Metrics: Level, Grade Level, Status, Assessed %
- Sub-components: Factual, Inference, Critical Thinking, Vocabulary, Grammar

### Reading Fluency (RF)
- Metrics: Status, % score, Code
- Sub-components: Letter sounds, CVC words, Blends, Consonant Diagraphs, Vowel Diagraphs, Magic E words, Multi-syllable words, Passages 1 & 2

### Mathematics
- Metrics: Level, Status, Mastery %
- Sub-components: Numbers, Patterns, Geometry, Mensuration, Time, Operations, Data Handling

### Proficiency Levels (all subjects)
`Developing` → `Beginner` → `Intermediate` → `Advanced`

---

## Data Flow Architecture

```
Raw Sources (Google Sheets / School App DB)
        │
        ▼
  Staging Layer          Clean, standardize, deduplicate raw data
  (*_stg models)         BTRIM, INITCAP, COALESCE, type casting
        │
        ▼
  Intermediate Layer     Join Baseline + Midline + Endline on student_id
  (*_fct, *_dim)         Fact tables (scores) + Dimension tables (student info)
        │
        ▼
  Production Layer       Aggregate by city/grade/school
  (analysis models)      Count students by level/status, calculate completion %
        │
        ▼
  Analytics Dashboards
```

---

## Naming Conventions

### Model Files
| Suffix | Meaning | Example |
|--------|---------|---------|
| `_stg` | Staging model | `baseline_2425_stg.sql` |
| `_fct` | Fact table | `base_mid_end_comb_scores_2425_fct.sql` |
| `_dim` | Dimension table | `base_mid_end_comb_students_2425_dim.sql` |

### Columns
- Output columns: `snake_case`
- Source columns from Google Sheets: PascalCase, always quoted — e.g., `b."Student ID"`
- Assessment timepoint suffixes: `_base`, `_mid`, `_end` (e.g., `rc_level_base`, `math_perc_numbers_mid`)

### Programs in Folder Names
Use these exact folder names: `AY 24_25`, `AY 25_26`, `EcoChamps_25_26`, `FSA_25_26`, `BF_SA_25_26`

---

## SQL Conventions

**Dialect:** PostgreSQL

**Standard patterns to use:**

```sql
-- Null handling
COALESCE(field, '')           -- string default
COALESCE(field, 0)            -- numeric default

-- Whitespace and casing
BTRIM(field)                  -- trim whitespace
INITCAP(field)                -- consistent title case

-- Type conversions
CASE WHEN val ~ '^\d+$' THEN val::INTEGER END          -- safe integer cast
REPLACE(val, '%', '')::NUMERIC                          -- percentage parsing

-- Deduplication
DISTINCT ON (student_id) ... ORDER BY student_id        -- deduplicate
WHERE "Student ID" <> ''                                -- remove empty rows

-- Attendance parsing
CASE WHEN val ~ '^P' THEN 'Present' ELSE 'Absent' END
```

**sqlfluff disabled rules:** L001, L029, RF05, LT05, ST06, RF02, ST10

---

## Key Macros

Always use these macros instead of writing custom logic:

### `validate_date(column)`
Handles 14+ date formats from inconsistent Google Sheets sources (DD/MM/YYYY, MM/DD/YYYY, DD-MM-YYYY, ISO timestamps, 2-digit years, etc.). Validates year range 1900–2050. Returns NULL for invalid dates.

```sql
{{ validate_date('"Assessment Date"') }}
```

### `mask_pii(column, type)`
Masks sensitive data. Supported types: `email`, `phone`, `name`.

```sql
{{ mask_pii('student_email', 'email') }}
```

### `generate_schema_name(custom_schema_name, node)`
Custom schema naming with prod/dev environment logic. Already configured in `dbt_project.yml` — do not modify without understanding the environment-specific behavior.

---

## Schema Documentation

- All models documented in `schema.yml` per layer (`staging/schema.yml`, `intermediate/schema.yml`, `prod/schema.yml`)
- Raw sources documented in `models/staging/sources.yml`
- **dbt-osmosis auto-manages `schema.yml`** via pre-commit hooks — do not manually reorganize these files
- Use `dbt_expectations` package for advanced column-level data quality tests

---

## Development Workflow

### Initial Setup
```bash
pip install -r requirements.txt
dbt deps
# Create a local profiles.yml for your database connection — this file is gitignored, never commit it
```

### Common Commands
```bash
dbt run                                    # Run all models
dbt run --select staging                   # Run staging layer only
dbt run --select +model_name              # Run model and all upstream deps
dbt run --select models/staging/AY\ 24_25 # Run one program's staging
dbt test                                   # Run all data quality tests
dbt build                                  # Run + test in one command
dbt build --full-refresh                   # Full rebuild (drops and recreates)
dbt docs generate && dbt docs serve        # Generate and browse documentation
sqlfluff lint models/                      # Lint SQL files
```

### Pre-commit Hooks
Run automatically on `git commit`:
- **dbt-osmosis YAML organize** — reorganizes schema.yml files
- **dbt-osmosis YAML refactor** — synthesizes column documentation from SQL

---

## CI/CD Pipeline

**File:** `.github/workflows/.dbt-ci.yml`

**Triggers:** Pull requests and pushes to any non-main branch

**Pipeline steps:**
1. Checkout code
2. Setup Python 3.8
3. Establish SSH tunnel to database
4. `pip install -r requirements.txt`
5. Generate `profiles.yml` from environment secrets
6. `dbt deps`
7. `dbt build --full-refresh`
8. `sqlfluff lint` (benchmarking mode)

**Required GitHub Secrets:**
```
POSTGRES_DBNAME
POSTGRES_USER
POSTGRES_PASSWORD
POSTGRES_HOST
SSH_PRIVATE_KEY
SSH_HOST
DB_USER
```

---

## Environment and Security

- **Never commit `profiles.yml`** — it is gitignored; contains database credentials
- **Never commit `.user.yml`** — gitignored; user-specific dbt config
- Use the `mask_pii` macro for any student PII fields (names, emails, phone numbers)
- Database access requires SSH tunneling (configured via `SSH_PRIVATE_KEY` in CI)
- Runtime credentials loaded via dbt's `env_var()` function

---

## Adding a New Program or Academic Year

1. Create program subfolders under `models/staging/`, `models/intermediate/`, and `models/prod/`
2. Add source definitions to `models/staging/sources.yml`
3. Create staging models (`_stg`) using standard SQL conventions above; use `validate_date` macro for all date columns
4. Create intermediate fact (`_fct`) and dimension (`_dim`) models joining assessment timepoints on `student_id`
5. Create production models aggregating by `city`, `grade`, `school`
6. Add model and column descriptions to the relevant `schema.yml` files
7. Use an existing program as a template — AY 25_26 closely mirrors AY 24_25 and is a good reference

---

## Useful Reference Files

| File | Purpose |
|------|---------|
| `models/staging/sources.yml` | All raw source table definitions |
| `models/staging/AY 24_25/baseline_2425_stg.sql` | Reference staging model for fellowship programs |
| `models/intermediate/AY 24_25/base_mid_end_comb_scores_2425_fct.sql` | Reference fact table joining 3 timepoints |
| `models/prod/AY 24_25/assessment_completion_2525.sql` | Reference production aggregation model |
| `models/staging/EcoChamps_25_26/eco_student25_26_stg.sql` | Reference for multi-source join + RAG pattern |
| `macros/validate_date.sql` | Date parsing macro — read before handling date columns |
| `dbt_project.yml` | Model materialization and project config |
| `.sqlfluff` | SQL linting rules and excluded checks |
