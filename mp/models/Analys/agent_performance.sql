{{ config(materialized='table') }}

WITH agent_summary AS (
    SELECT
        "sales_agent_names",
        AVG("Total Revenue") AS avg_revenue,
        COUNT(*) AS total_sales,
        AVG("Discount Amount ($)") AS avg_discount
    FROM {{ ref('fct_sales') }}
    GROUP BY "sales_agent_names"
)

SELECT
    "sales_agent_names",
    avg_revenue,
    total_sales,
    avg_discount,
    RANK() OVER (ORDER BY avg_revenue DESC) AS rank
FROM agent_summary
