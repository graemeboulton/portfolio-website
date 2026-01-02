---
date: 2025-12-19 06:20:35 +0301
title: Data engineering with Azure & Databricks
subtitle: ETL using Azure and Databricks with a medallion architecture (In Progress)
image: '/images/data-engineering-project.svg'
hide_image: true
---

## Project background

This project demonstrates the design and implementation of a **production-style, end-to-end data engineering pipeline** using Microsoft Azure services. The objective was to ingest data from an on-premises SQL Server, apply scalable transformations using modern data engineering patterns, and deliver analytics-ready data to business users through Power BI.

The project mirrors real-world enterprise data platforms, focusing on automation, security, and analytics enablement rather than isolated tooling..

---

## Infrastructure & Architecture

![Jobs Pipeline](/images/data-engineering-pipeline.svg)

---

## Solution Overview

To address these requirements, a cloud-based data platform was built on Azure with the following objectives:

- Securely ingest data from an on-premises SQL Server
- Apply structured transformations using a medallion architecture
- Serve analytics-ready data via SQL
- Enable interactive business reporting
- Automate and govern the entire pipeline

---

## Architecture & Pipeline Flow

### 1. Data Ingestion (Bronze Layer)

- Azure Data Factory connects to the on-premises SQL Server using a **self-hosted integration runtime**
- Table metadata is dynamically retrieved to avoid hardcoding
- Source tables are extracted and stored as **Parquet files** in the Bronze layer of Azure Data Lake Storage

This approach allows the pipeline to automatically adapt to schema or table changes.

---

### 2. Data Transformation – Silver Layer

- Azure Databricks notebooks perform initial cleansing and standardisation
- Data types are normalised and schemas aligned
- Outputs are written in **Delta format**, enabling:
  - Schema evolution
  - Version tracking
  - Reliable overwrite and append operations

The Silver layer represents clean, standardised source data.

---

### 3. Data Transformation – Gold Layer

- Business-level transformations are applied
- Tables are reshaped into analytics-friendly structures
- Column names are standardised (e.g. `UPPER_SNAKE_CASE`)
- Outputs follow **fact and dimension-style patterns**

The Gold layer acts as the single source of truth for analytics and reporting.

---

### 4. Orchestration & Automation

- Azure Data Factory orchestrates the full pipeline end-to-end
- Databricks notebooks are triggered directly from ADF
- Pipelines are scheduled to run automatically on a daily basis
- Monitoring and debugging are handled through Azure-native tooling

---

### 5. Data Serving with Synapse Serverless SQL

- Azure Synapse Serverless SQL queries data directly from the Gold layer
- SQL views are created over Delta tables using `OPENROWSET`
- No data is duplicated or copied into a traditional warehouse

This approach provides SQL accessibility with minimal infrastructure cost.

---

### 6. Reporting & Analytics

- Power BI connects to Synapse Serverless SQL
- SQL views are imported into the Power BI data model
- Relationships are defined between fact and dimension tables
- Interactive dashboards provide insights into:
  - Total sales
  - Product performance
  - Customer demographics
  - Gender-based purchasing patterns

---

## Security & Governance

- Secrets (database credentials, access tokens) are stored in **Azure Key Vault**
- Managed identities are used instead of hard-coded credentials
- Access is controlled using **Microsoft Entra ID security groups**
- Role-based access control (RBAC) is applied at the resource group level

This setup reflects enterprise-grade security and governance practices.

---

## Outcome

The final solution delivers a **fully automated, scalable, and secure data platform** that transforms raw operational data into actionable business insights.

This project demonstrates hands-on experience with:
- Cloud data ingestion
- Distributed data processing
- Data modelling for analytics
- Pipeline orchestration
- Security and governance
- Business-facing reporting

---

## Future Enhancements

- Implement incremental loads or change data capture (CDC)
- Add data quality validation checks
- Introduce CI/CD for pipeline deployment
- Integrate monitoring and alerting
- Extend the model to support additional data sources