{{ config(materialized='table') }}

WITH average_discount AS (
    SELECT
        AVG("Discount Amount ($)") AS avg_discount
    FROM {{ ref('fct_sales') }}
),

agent_discount AS (
    SELECT
        "Sales Agent Name",
        ROUND(
            AVG("Discount Amount ($)")::numeric,
            1
         ) AS avg_agent_discount
    FROM {{ ref('fct_sales') }}
    GROUP BY "Sales Agent Name"
),

agents_above_avg AS (
    SELECT
        ad."Sales Agent Name",
        ad.avg_agent_discount
    FROM agent_discount ad
    JOIN average_discount avd
    ON ad.avg_agent_discount > avd.avg_discount
)

SELECT
    "Sales Agent Name",
    avg_agent_discount
FROM agents_above_avg
ORDER BY avg_agent_discount DESC
