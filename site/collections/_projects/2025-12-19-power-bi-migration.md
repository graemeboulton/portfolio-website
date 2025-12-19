---
date: 2025-12-19 05:20:35 +0300
title: Power BI Migration
subtitle: Migration from SAP BusinessObjects to Power BI
image: '/images/power-bi-migration.svg'
hide_image: true
---

## Project background

This project took place during my previous role. Due to confidentiality, I’m unable to share the final reports themselves, but I can explain the approach, decisions, and architecture used to successfully migrate from SAP BusinessObjects to Power BI.

The journey began in early 2020, during the first Covid lockdown, while I was on furlough. With the business facing unprecedented disruption, I was keen to use the time productively and saw a clear opportunity to modernise our reporting capability. Around this time, the organisation had just purchased its first Power BI licence.

I started by experimenting with a lightweight, locally run ETL process. This involved extracting footfall data from an in-house system, transforming it in Power BI Desktop using a combination of Power Query and DAX, and building an initial set of reports. While rudimentary by today’s standards, this was my first exposure to Power BI’s full potential.

After a period of trial and error, I was genuinely surprised by how powerful and flexible Power BI already was, particularly compared to our existing reporting tools. That initial proof of concept became the foundation for a much broader BI transformation.

---

## Why I felt we had to modernise

The SAP BusinessObjects environment was built around a centrally governed, on-premise reporting model. While it had served the business well historically, the organisation’s analytics needs had evolved significantly.

There was increasing demand for faster insight, greater self-service, and improved alignment with modern, cloud-based data platforms. Over time, the reliance on complex universes, static reports, and specialist maintenance created bottlenecks and limited agility.

I therefore chose to modernise the BI stack to reduce operational overhead, improve scalability, and better support how analytics were now being consumed across the business — particularly during the post-Covid trading environment.

---

## Scope of the migration

I initially prioritised a core suite of retail performance reports covering:

- Sales  
- Footfall  
- Conversion  
- A broad range of employee KPIs  

Together, these provided a consolidated and timely view of retail performance, which was critical during the post-Covid recovery period.

Once this foundation was in place, I expanded the scope to modernise reporting for board-level consumption. This included reports across:

- Retail  
- Merchandise  
- Marketing  
- HR  
- Technology  

These were consolidated into a single Power BI App, providing leadership with a consistent, trusted view of performance across the organisation.

---

## Semantic model design

At the time, the business had limited historical data retention. As a result, I designed a single core semantic model to support all retail sales and KPI reporting, ensuring consistency and reducing duplication of logic.

In parallel, I built separate, function-specific models for each area included in the board reporting suite. This approach balanced reuse and simplicity, while allowing each business function to evolve its reporting independently without impacting others.

---

## Data sources

Data was sourced from a wide range of systems, including:

- An on-premise ERP system  
- Multiple cloud-based, in-house applications  
- SharePoint, used to supplement reporting where systems were not directly accessible via Power BI or where data was maintained in spreadsheets  

This hybrid sourcing model was typical of the organisation at the time and required careful standardisation and validation to ensure consistency across reports.

---

## Rebuilding logic and calculations

A significant part of the migration effort involved rebuilding business logic. Hundreds of measures and calculated columns were created to support reporting requirements.

This was a substantial undertaking, as it required reverse-engineering many existing SAP BusinessObjects reports to understand:

- Where data originated  
- How calculations were performed  
- Which filters and assumptions were applied  

All business logic was progressively centralised into Power BI semantic models, reducing report-level complexity and improving consistency.

---

## Security and governance

To ensure consistency, security, and ease of access, I routed all data through Power BI Dataflows. This allowed reports to be built entirely within the Power BI Service, without requiring end users to manage data source credentials.

This approach ensured:

- A single version of the truth  
- Consistent transformations across reports  
- Improved governance and reduced operational risk  

---

## Performance optimisation

Running models via Dataflows provided greater control over refresh schedules and data processing. This ensured that reporting workloads had no adverse operational impact on source systems while still meeting business expectations around data freshness.

Incremental and scheduled refresh strategies were used where appropriate to balance performance and availability.

---

## Architecture (high level)

- On-premise data accessed via Power BI Gateway  
- Power BI Dataflows (Gen1) for data ingestion and transformation  
- Centralised semantic models reused across reports  
- Distribution via Power BI Apps for controlled access  

---

## Reporting outputs

### Board-level reporting (via Power BI App)

- Retail  
- Merchandise  
- HR  
- Marketing  
- Technology  

### Operational reporting

- Retail KPIs  
  - Sales  
  - Footfall  
  - Conversion  
- Marketing performance  
- Employee-related KPIs  