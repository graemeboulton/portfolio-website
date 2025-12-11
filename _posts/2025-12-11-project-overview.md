---
title: "Jobs insight project is now live - here's a high level overview of how it was created"
date: 2025-12-11 12:00:00 +0000
categories:
  - blog
published: true
---

Here is a high-level summary of this project. I will post a deep-dive into the finer details, but for now it explains how it was pulled together.

**👉 [View the full project dashboard](https://www.graemeboulton.com/project/)**  

## Reed Job Pipeline — Project Summary


This project is an end-to-end data pipeline built to ingest job-market data from the Reed.co.uk API, transform and enrich it, and surface insights through a dimensional PostgreSQL warehouse and Power BI.

It demonstrates experience across API integration, Python ETL, data modelling, analytics engineering, and dashboarding.

### 1) Architecture Summary

**Data Ingestion (Reed API)**

* Python-based pipeline with configurable search terms, pagination, and incremental refresh
* API key rotation to avoid rate limits, with exponential backoff retries
* Incremental ingestion via `POSTED_BY_DAYS`
* Enrichment from the detail endpoint for full descriptions
* Up to 100 results/page with optional caps (e.g., `MAX_RESULTS`)

**Data Transformation & Enrichment**

* Title filtering rules to target data roles and exclude noise
* Skill extraction (regex) across languages, BI, cloud, databases, tooling
* Skill normalisation (e.g., “postgres” → “postgresql”)
* Classification for seniority, working pattern, contract type/hours
* Salary annualisation (day/week/hour/month → annum)
* Role categorisation (Analyst, Engineer, Scientist, Architect, etc.)

### 2) Data Warehouse Design

**Schemas & Tables**

**Landing**

* `landing.raw_jobs`: JSONB of untouched API responses (hashing for change detection)

**Staging**

* `staging.jobs_v1`: flattened job records with transformation outputs

**Analytics**

* `staging.fact_jobs`: materialised, enriched fact table (annualised salaries, dimensional keys, audit fields)

**Dimensions**

* `dim_salaryband`, `dim_seniority`, `dim_contract`, `dim_salarytype`,
  `dim_jobtype`, `dim_location`, `dim_employer`, `dim_demandband`, `dim_ageband`

**Skills**

* `staging.job_skills`: junction table mapping jobs to extracted skills (with categories)

### 3) Data Quality & Performance

* Deduplication via compound keys (`source_name`, `job_id`)
* Automatic expiry handling for `expires_at`
* Description completeness checks
* Indexing strategy for PBI performance (e.g., salary band)
* Incremental refresh logic for materialised views

### 4) Configuration

Configurable via environment variables:

* Keywords, title filters, exclusion rules
* Skill patterns and alias mappings
* Role classification rules
* Incremental vs backfill modes
* API limits & pagination
* Salary conversion factors

### 5) Recent Improvements

* Corrected salary annualisation factors + historical backfill
* Expanded exclusion list (e.g., lab analysts, data protection, QA/QC, recruitment)
* Historical refresh from landing without re-ingest
* Improved job-type detection and salary band alignment

### 6) Stack

* **Python 3.x** (requests, regex, JSON, custom ETL)
* **Azure Functions** (timer-trigger)
* **PostgreSQL** (JSONB, views/materialised views, dimensional modelling)
* **Power BI** (visuals, Power Query/M, data modelling)

### 7) Scale & Metrics

* ~2,800 job posts processed
* 60+ canonical skills across 6+ domains
* Salary bands £0–£540k (10k steps)
* Continuous ingestion with multi-key resiliency
