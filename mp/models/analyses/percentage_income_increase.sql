{{ config(materialized='table') }}

WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', "Order Date Kyiv") AS month,
        ROUND(
            SUM("Total Amount ($)")::numeric,
            1
        ) AS total_revenue
    FROM {{ ref('fct_sales') }}
    GROUP BY month
),

revenue_growth AS (
    SELECT
        month,
        total_revenue,
        LAG(total_revenue) OVER (ORDER BY month) AS prev_month_revenue,
        ROUND(
            ((total_revenue - LAG(total_revenue) OVER (ORDER BY month)) / LAG(total_revenue) OVER (ORDER BY month))::numeric * 100,
            1
            )
             AS revenue_growth_percentage
    FROM monthly_revenue
)

SELECT
    month,
    total_revenue,
    prev_month_revenue,
    revenue_growth_percentage
FROM revenue_growth
ORDER BY month
