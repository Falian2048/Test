{{ config(materialized='table') }}

WITH overall_avg_discount AS (
    SELECT
        AVG("Discount Amount ($)") AS avg_discount
    FROM {{ ref('fct_sales') }}
),
agent_discounts AS (
    SELECT
        "sales_agent_names",
        AVG("Discount Amount ($)") AS avg_discount
    FROM {{ ref('fct_sales') }}
    GROUP BY "sales_agent_names"
)

SELECT
    a."sales_agent_names",
    a.avg_discount
FROM agent_discounts a
JOIN overall_avg_discount o
ON a.avg_discount > o.avg_discount
