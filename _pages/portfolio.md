---
title: "Portfolio"
permalink: /portfolio/
---

# My Portfolio

Welcome to my data portfolio. This page showcases sample SQL queries, interactive Power BI reports, and links to PBIX files you can download and explore.

## Sample SQL Query

```sql
-- Example: Query total sales by month
SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(total_amount) AS total_sales
FROM orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;
```

## Embedding Power BI Reports

You can embed interactive Power BI reports using an `<iframe>` if you have published the report to the Power BI service and enabled embedding. Replace `YOUR_EMBED_URL` with the actual embed URL for your report:

```html
<iframe title="Sales Dashboard"
    width="800" height="600"
    src="https://app.powerbi.com/view?r=YOUR_EMBED_URL"
    frameborder="0" allowFullScreen="true"></iframe>
```

Alternatively, you can provide a direct link to the report:

[View Sales Dashboard](https://app.powerbi.com/view?r=YOUR_EMBED_URL)

## PBIX Downloads

If you want to share downloadable PBIX files, you can add them to your repository (for example, in `assets/files`) and link to them here:

- [Download Sales Dashboard PBIX](../assets/files/sales_dashboard.pbix)
