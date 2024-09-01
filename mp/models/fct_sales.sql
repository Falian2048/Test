{{ config(materialized='table') }}

WITH cleaned_data AS (
    SELECT
        "Reference ID",
        "Campaign Name",
        "Product Name",
        "Country",
        "Source",
        "Sales Agent Name",
        TO_TIMESTAMP("Order Date Kyiv", 'Month DD, YYYY, HH12:MI') AS "Order Date Kyiv",
        TO_TIMESTAMP("Return Date Kyiv", 'Month DD, YYYY, HH12:MI') AS "Return Date Kyiv",
        COALESCE("Total Amount ($)"::double precision, 0) AS "Total Amount ($)",
        COALESCE("Total Rebill Amount"::double precision, 0) AS "Total Rebill Amount",
        COALESCE("Number Of Rebills"::integer, 0) AS "Number Of Rebills",
        COALESCE("Discount Amount ($)"::double precision, 0) AS "Discount Amount ($)",
        COALESCE("Returned Amount ($)"::double precision, 0) AS "Returned Amount ($)"
    FROM {{ source('my_database', 'main') }}
),

ranked_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY "Reference ID" ORDER BY "Order Date Kyiv") AS rn
    FROM cleaned_data
)

SELECT
    COALESCE("Reference ID", 'N/A') AS "Reference ID",
    COALESCE("Campaign Name", 'N/A') AS "Campaign Name",
    COALESCE("Product Name", 'N/A') AS "Product Name",
    COALESCE("Country", 'N/A') AS "Country",
    COALESCE("Source", 'N/A') AS "Source",
    COALESCE("Sales Agent Name", 'N/A') AS "Sales Agent Name",
    COALESCE("Total Amount ($)", 0) AS "Total Amount ($)",
    COALESCE("Total Rebill Amount", 0) AS "Total Rebill Amount",
    COALESCE("Number Of Rebills", 0) AS "Number Of Rebills",
    COALESCE("Discount Amount ($)", 0) AS "Discount Amount ($)",
    COALESCE("Returned Amount ($)", 0) AS "Returned Amount ($)",
    CASE
        WHEN "Return Date Kyiv" IS NULL THEN NULL
        ELSE "Return Date Kyiv" AT TIME ZONE 'Europe/Kiev'
    END AS "Return Date Kyiv",
    CASE
        WHEN "Return Date Kyiv" IS NULL THEN NULL
        ELSE "Return Date Kyiv" AT TIME ZONE 'UTC'
    END AS return_date_utc,
    CASE
        WHEN "Return Date Kyiv" IS NULL THEN NULL
        ELSE "Return Date Kyiv" AT TIME ZONE 'America/New_York'
    END AS return_date_new_york,
    CASE
        WHEN "Order Date Kyiv" IS NULL THEN NULL
        ELSE "Order Date Kyiv" AT TIME ZONE 'Europe/Kiev'
    END AS "Order Date Kyiv",
    CASE
        WHEN "Order Date Kyiv" IS NULL THEN NULL
        ELSE "Order Date Kyiv" AT TIME ZONE 'UTC'
    END AS order_date_utc,
    CASE
        WHEN "Order Date Kyiv" IS NULL THEN null
        ELSE "Order Date Kyiv" AT TIME ZONE 'America/New_York'
    END AS order_date_new_york,
    COALESCE(
        EXTRACT(DAY FROM ("Return Date Kyiv" - "Order Date Kyiv")),
        0
    ) AS days_between_return_and_order
FROM ranked_data
WHERE rn = 1
