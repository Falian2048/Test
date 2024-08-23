{{ config(materialized='table') }}

WITH monthly_revenue AS (
    SELECT
        date_trunc('month', "Order Date Kyiv") AS month,
        SUM("Total Revenue") AS total_revenue
    FROM {{ ref('fct_sales') }}
    GROUP BY month
),
revenue_growth AS (
    SELECT
        month,
        total_revenue,
        LAG(total_revenue) OVER (ORDER BY month) AS previous_month_revenue
    FROM monthly_revenue
)

SELECT
    month,
    total_revenue,
    previous_month_revenue,
    CASE
        WHEN previous_month_revenue IS NULL THEN NULL
        ELSE ((total_revenue - previous_month_revenue) / previous_month_revenue) * 100
    END AS revenue_growth_percentage
FROM revenue_growth
