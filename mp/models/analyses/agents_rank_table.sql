{{ config(materialized='table') }}

WITH agent_metrics AS (
    SELECT
        "Sales Agent Name",
        COUNT(*) AS number_of_sales,
        ROUND(
            AVG("Total Amount ($)")::numeric,
            1
            ) AS average_revenue,
        ROUND(
            AVG("Discount Amount ($)")::numeric,
            1
         ) AS average_discount
    FROM {{ ref('fct_sales') }}
    GROUP BY "Sales Agent Name"
),

ranked_agents AS (
    SELECT
        "Sales Agent Name",
        number_of_sales,
        average_revenue,
        average_discount,
        RANK() OVER (ORDER BY average_revenue DESC) AS rank
    FROM agent_metrics
)

SELECT
    "Sales Agent Name",
    number_of_sales,
    average_revenue,
    average_discount,
    rank
FROM ranked_agents
ORDER BY rank
