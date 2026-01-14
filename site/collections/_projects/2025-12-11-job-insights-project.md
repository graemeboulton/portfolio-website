---
date: 2025-12-11 05:20:35 +0300
title: UK data job market insights v1
subtitle: Serverless ETL using Python Azure Functions, SQL, Power Query, DAX & Power BI.
image: '/images/job-insights-hero.png'
hide_image: true
---
<div style="text-align: center;">
<iframe title="Jobs 1" width="700" height="500"  src="https://app.powerbi.com/view?r=eyJrIjoiMGVhYTAxNjgtOTFiMy00MzVkLWJlYzMtMDIzZDI5N2NkOGFkIiwidCI6IjVkNWVkNWRjLTE1YTctNDljMi05OWU2LWQzYmQ1NzY0YjM1NiJ9&pageName=801249cc78928d02bb84" frameborder="0" allowFullScreen="true"></iframe>
</div>
---

## Project background

With this project, I wanted to both demonstrate a broad range of data and BI skills and gain practical insights into the job market to support my own career transition.

---

## Data selection

Initially, I considered using sample datasets from Kaggle or Hugging Face, but after researching publicly available options, the Reed.co.uk API stood out as a richer, more realistic source for a project centred around analytics, ETL, and reporting.

---

## Architecture

![Jobs Pipeline](/images/jobs-pipeline.svg)

```
┌─────────────────┐
│  Reed.co.uk API │  ← HTTP Basic Auth, 4-key rotation, rate-limit handling
└────────┬────────┘
         │ fetch (pagination & incremental filtering)
         ▼
┌─────────────────────────────────┐
│  landing.raw_jobs (JSONB raw)   │  ← Raw API responses, content hashing
│  Deduplication via composite PK │
└────────┬────────────────────────┘
         │ transform & enrich (Python ETL)
         ▼
┌──────────────────────────────────┐
│  staging.jobs_v1 (flattened)     │  ← Normalized schema, skill extraction
│  Enriched metadata & seniority   │
└────────┬───────────────────────┬─┘
         │                       │
         ▼                       ▼
┌──────────────────┐  ┌────────────────────┐
│ staging.fact_jobs│  │ staging.job_skills │  ← Many-to-many junction table
│ Materialized w/  │  │ 60+ canonical skills│
│ dimensional keys │  │ + categories        │
└────────┬─────────┘  └────────────────────┘
         │
         ▼
┌─────────────────┐
│ Power BI/Fabric │  ← Interactive dashboards & analytics
│   Analytics     │
└─────────────────┘
```

---

## Key Features & Capabilities

### Robust API Integration
- **4-tier API key rotation** with round-robin load balancing
- **Intelligent rate-limit handling** (403 errors → automatic fallback)
- **Adaptive retry logic** with exponential backoff (500/502/503 errors)
- **Incremental ingestion** (configurable lookback window: fetch only jobs posted in last N days)
- **Content-hash change detection** (MD5 hashing prevents unnecessary reprocessing)

### Smart Data Enrichment
- **Auto-fetches full job descriptions** from detail endpoint (works around Reed's 453-char truncation)
- **Description validation** detects truncation patterns and validates completeness
- **60+ canonical skills extraction** with regex patterns and normalization (e.g., `postgres` → `postgresql`, `k8s` → `kubernetes`)
- **Skill categorization**: Programming Languages, Databases, Cloud/DevOps, ML, BI, Tools
- **Seniority detection**: Executive/Director/Manager/Lead/Senior/Mid/Junior/Entry
- **Work location classification**: Remote, Hybrid, Office
- **Employment type extraction**: Full-time, Part-time, Contract
- **Job role categorization**: Engineering, Analyst, Scientist, Architect

### Intelligent Filtering & Quality
- **Rule-based title filtering** with word-boundary matching (configurable include/exclude lists)
- **Optional ML classifier** (TF-IDF + Naive Bayes) for advanced job relevance scoring
- **Deduplication** via composite primary keys at every layer
- **Expired job cleanup** (automatic removal based on expiration dates)
- **Data quality metrics** (enrichment rate, description validation, duplicate detection)
- **Blacklist support** for permanently excluded job IDs

### Salary Normalization & Analytics
- **Salary annualization** converts all salary types to annual figures:
  - `per week` × 52 weeks/year
  - `per day` × 260 working days/year  
  - `per hour` × 1,950 working hours/year
- **Dynamic salary bands**: £0–9,999, £10k–19,999, ..., capped at £540k–549,999
- **Original value preservation** (`*_old` columns for audit trail)
- Enables consistent cross-title salary comparisons

### Dimensional Data Warehouse
- **Star schema design** with fact and dimension tables
- **Materialized analytics table** (`fact_jobs`) with pre-computed dimensional keys
- **10+ dimension tables**: Salary bands, employers, locations, seniority, contracts, skills, etc.
- **Indexed for performance** (primary keys + salary band index)
- **Atomic UPSERT operations** (ON CONFLICT pattern with temp tables)
- **Batch processing** (500–1,000 rows per insert for optimal throughput)

---

## Technical Stack

| Layer | Technology | Purpose |
|-------|------------|---------|
| **Language** | Python 3.x | ETL logic, data transformations |
| **Cloud Platform** | Azure Functions | Serverless compute (timer-triggered) |
| **Database** | PostgreSQL | Data warehouse (Azure Database for PostgreSQL) |
| **API Client** | `requests` | HTTP client with authentication |
| **Data Processing** | `psycopg2` | PostgreSQL driver with batch operations |
| **ML (Optional)** | `scikit-learn` | TF-IDF + Naive Bayes job classifier |
| **Visualization** | Power BI / Microsoft Fabric | Interactive dashboards |
| **Deployment** | Azure | Managed infrastructure |

---

## Project Structure

```
jobs-pipeline/
├── function_app.py              # Azure Functions entry point (timer trigger)
├── run_pipeline.py              # Local test runner for development
├── requirements.txt             # Python dependencies
├── local.settings.json          # Environment configuration (gitignored)
│
├── reed_ingest/                 # Main ETL module (2,500+ lines)
│   └── __init__.py              ├─ API client with 4-key rotation
│                                ├─ Skill extraction & categorization
│                                ├─ Title filtering (rule-based + ML)
│                                ├─ Salary annualization logic
│                                ├─ Seniority/location/employment detection
│                                ├─ Data quality monitoring
│                                └─ Database UPSERT operations
│
├── job_classifier.py            # TF-IDF + Naive Bayes ML classifier (optional)
│
├── sql/                         # Database schema definitions
│   ├── fact_jobs.sql            ├─ Materialized analytics table
│   ├── fact_job_skill.sql       ├─ Job-skill junction table
│   ├── dim_salaryband.sql       ├─ Dynamic salary bands
│   ├── dim_employer.sql         ├─ Employer dimension
│   ├── dim_location.sql         ├─ Location dimension
│   ├── dim_seniority.sql        ├─ Seniority levels
│   ├── dim_skill.sql            ├─ Canonical skills
│   └── ...                      └─ Other dimensions
│
├── docs/                        # Project documentation
│   ├── project_structure.md     ├─ Repository organization
│   ├── duplication_prevention.md├─ Data quality strategies
│   └── recent_changes.md        └─ Change log
│
└── powerbi/                     # Power BI visualizations
    └── README.md
```

---

## Data Flow

### 1. Extract (API Ingestion)
- **Fetch jobs** from Reed.co.uk API with pagination (50–100 results per page)
- **Incremental filtering** (only jobs posted in last N days via `postedByDays` parameter)
- **Round-robin key rotation** distributes load across 4 API keys
- **Rate-limit handling** automatically falls back to backup keys on 403 errors
- **Store raw JSON** in `landing.raw_jobs` with content hash for change detection

### 2. Transform (Data Enrichment)
- **Title filtering** applies include/exclude rules with word-boundary matching
- **Skill extraction** identifies 60+ canonical skills from descriptions
- **Skill normalization** maps variations to standard names (e.g., `powerbi` → `power bi`)
- **Seniority detection** infers level from title/description keywords
- **Work location classification** determines remote/hybrid/office
- **Salary annualization** converts all salary types to annual figures
- **Job role categorization** assigns Engineering/Analyst/Scientist/Architect

### 3. Load (Database Operations)
- **Atomic UPSERT** to `staging.jobs_v1` via temp table pattern
- **Batch inserts** (500–1,000 rows per operation)
- **Skill extraction** to `staging.job_skills` junction table
- **Dimension population** (employers, locations, seniority, etc.)
- **Fact table materialization** (`fact_jobs`) with pre-computed dimensional keys

### 4. Analytics (Power BI)
- **Direct Query** or **Import mode** connections to PostgreSQL
- **Pre-computed metrics** (days open, apps/day, competition analysis)
- **Dimensional analysis** (salary bands, seniority, skills, locations)
- **Interactive dashboards** with filters and drill-downs

---

## Database Schema

### Landing Layer
**`landing.raw_jobs`** - Raw API responses
```sql
CREATE TABLE landing.raw_jobs (
    source_name     TEXT,           -- 'reed'
    job_id          TEXT,           -- Reed job ID
    raw             JSONB,          -- Complete API JSON
    content_hash    TEXT,           -- MD5 for change detection
    posted_at       TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,
    ingested_at     TIMESTAMPTZ,
    PRIMARY KEY (source_name, job_id)
);
```

### Staging Layer
**`staging.jobs_v1`** - Normalized job records
```sql
CREATE TABLE staging.jobs_v1 (
    staging_id          BIGSERIAL PRIMARY KEY,
    source_name         TEXT,
    job_id              TEXT,
    job_title           TEXT,
    employer_name       TEXT,
    location_name       TEXT,
    salary_min          NUMERIC,        -- Annualized
    salary_max          NUMERIC,        -- Annualized
    salary_type         TEXT,
    work_location_type  TEXT,           -- remote/hybrid/office
    seniority_level     TEXT,
    job_role_category   TEXT,
    contract_type       TEXT,
    full_time           BOOLEAN,
    part_time           BOOLEAN,
    job_description     TEXT,           -- Full enriched text
    posted_at           TIMESTAMPTZ,
    expires_at          TIMESTAMPTZ,
    UNIQUE (source_name, job_id)
);
```

**`staging.job_skills`** - Job-to-skill mappings
```sql
CREATE TABLE staging.job_skills (
    id              BIGSERIAL PRIMARY KEY,
    source_name     TEXT,
    job_id          TEXT,
    skill           TEXT,               -- Canonical skill name
    category        TEXT,               -- programming_languages, databases, etc.
    matched_pattern TEXT,               -- Original variation matched
    UNIQUE (source_name, job_id, skill)
);
```

### Analytics Layer
**`staging.fact_jobs`** - Materialized analytics table (indexed)
- All job attributes + pre-computed dimensional keys
- Salary bands, employer keys, location keys, etc.
- Computed metrics: `days_open`, `apps_per_day`, `is_active`
- Indexed on `salaryband_key` for fast filtering

**Dimension Tables** (10+ tables)
- `dim_salaryband` - Dynamic £10k-width bands (£0–£549,999, capped)
- `dim_employer` - Employer master data
- `dim_location` - Location + work type (remote/hybrid/office)
- `dim_seniority` - Seniority levels
- `dim_contract` - Contract types
- `dim_skill` - Canonical skill names
- `dim_source` - Data sources (Reed, etc.)
- `dim_ageband` - Job age bands
- `dim_demandband` - Application volume bands
- `dim_jobtype` - Role categories

---

## Configuration

All settings are environment-driven via `local.settings.json`:

```json
{
  "Values": {
    "API_KEY": "your-reed-api-key",
    "API_KEY_BACKUP": "backup-key-1",
    "API_KEY_BACKUP_2": "backup-key-2",
    "API_KEY_BACKUP_3": "backup-key-3",
    "API_BASE_URL": "https://www.reed.co.uk/api/1.0/search",
    "SEARCH_KEYWORDS": "data,bi,analyst,microsoft fabric",
    "RESULTS_PER_PAGE": 100,
    "POSTED_BY_DAYS": 30,
    "MAX_RESULTS": 0,
    "JOB_TITLE_INCLUDE": "data,bi,analyst,microsoft fabric",
    "JOB_TITLE_EXCLUDE": "trainee,intern,apprentice,asbestos,...",
    "USE_ML_CLASSIFIER": "false",
    "ML_CLASSIFIER_THRESHOLD": "0.7",
    "PGHOST": "your-postgres-server.postgres.database.azure.com",
    "PGPORT": "5432",
    "PGDATABASE": "jobs_warehouse",
    "PGUSER": "admin",
    "PGPASSWORD": "****",
    "PGSSLMODE": "require"
  }
}
```

---

## Running the Pipeline

### Local Development
```bash
# Install dependencies
pip install -r requirements.txt

# Configure settings
cp local.settings.json.example local.settings.json
# Edit local.settings.json with your API keys and database credentials

# Run pipeline locally
python run_pipeline.py
```

### Azure Deployment
```bash
# Deploy to Azure Functions
func azure functionapp publish <your-function-app-name>

# Scheduled execution (daily at midnight UTC)
# Configured in function_app.py: @app.timer_trigger(schedule="0 0 0 * * *")
```

---

## Key Metrics & Performance

### Current System Stats
- **Total jobs processed**: 2,791 unique postings
- **Staging layer**: 2,552 jobs (after filtering)
- **Skills extracted**: 60+ canonical skills across 6 categories
- **Dimensions**: 10+ dimensional tables
- **Deduplication**: 0 duplicates across all layers
- **Enrichment rate**: >95% full descriptions fetched

### Performance Optimizations
- **Indexed queries** (primary keys + salary band index)
- **Materialized fact table** (eliminates expensive joins)
- **Batch inserts** (500–1,000 rows per operation)
- **Atomic transactions** (temp table pattern for consistency)
- **Content-hash caching** (avoids unnecessary reprocessing)
- **Round-robin key rotation** (distributes API load evenly)

---

## Skills Demonstrated

### Data Engineering
- **ETL Pipeline Design** - End-to-end data flow from API to warehouse
- **API Integration** - Rate limiting, retry logic, authentication
- **Data Warehousing** - Star schema, dimensional modeling, indexing
- **Data Quality** - Deduplication, validation, change detection
- **Performance Optimization** - Batch processing, materialized views, indexing

### Python Development
- **Clean Code** - Modular design, type hints, docstrings
- **Error Handling** - Robust exception management, logging
- **Testing** - Local test runner for development
- **Configuration Management** - Environment-driven settings

### Database Engineering
- **PostgreSQL** - DDL, DML, CTEs, window functions
- **Schema Design** - Normalization, foreign keys, constraints
- **Query Optimization** - Indexes, materialized views, batch operations
- **ACID Transactions** - Atomic operations, consistency guarantees

### Cloud & DevOps
- **Azure Functions** - Serverless compute, timer triggers
- **Azure Database for PostgreSQL** - Managed database service
- **CI/CD** - Automated deployment pipeline
- **Security** - Secrets management, SSL connections

### Analytics & BI
- **Power BI** - Dashboard design, DAX measures, data modeling
- **Dimensional Modeling** - Star schema, fact/dimension tables
- **Business Metrics** - KPIs, trends, competitive analysis


