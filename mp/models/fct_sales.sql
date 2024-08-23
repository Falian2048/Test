{{ config(materialized='table') }}

WITH cleaned_data AS (
    SELECT
        "Campaign Name",
        "Product Name",
        "Country",
        "Source",
        "Sales Agent Name",
        -- Преобразование текста в формат даты
        TO_TIMESTAMP("Order Date Kyiv", 'Month DD, YYYY, HH12:MI') AS order_date_kyiv,
        TO_TIMESTAMP("Return Date Kyiv", 'Month DD, YYYY, HH12:MI') AS return_date_kyiv,
        COALESCE("Total Amount ($)"::double precision, 0) AS "Total Amount ($)",
        COALESCE("Total Rebill Amount"::double precision, 0) AS "Total Rebill Amount",
        COALESCE("Number Of Rebills"::integer, 0) AS "Number Of Rebills",
        COALESCE("Discount Amount ($)"::double precision, 0) AS "Discount Amount ($)",
        COALESCE("Returned Amount ($)"::double precision, 0) AS "Returned Amount ($)"
    FROM {{ source('my_database', 'main') }}
),

aggregated_data AS (
    SELECT
        "Campaign Name",
        "Product Name",
        "Country",
        "Source",
        STRING_AGG("Sales Agent Name", ', ') AS "sales_agent_names",
        COALESCE(SUM("Total Amount ($)"), 0) AS "Total Amount ($)",
        COALESCE(SUM("Total Rebill Amount"), 0) AS "Total Rebill Amount",
        COALESCE(SUM("Number Of Rebills"), 0) AS "Number Of Rebills",
        COALESCE(SUM("Discount Amount ($)"), 0) AS "Discount Amount ($)",
        COALESCE(SUM("Returned Amount ($)"), 0) AS "Returned Amount ($)",
        -- Дата возврата в разных временных зонах
        return_date_kyiv AT TIME ZONE 'Europe/Kiev' AS return_date_kyiv_utc,
        return_date_kyiv AT TIME ZONE 'UTC' AS return_date_utc,
        return_date_kyiv AT TIME ZONE 'America/New_York' AS return_date_new_york,
        -- Дата продажи в разных временных зонах
        order_date_kyiv AT TIME ZONE 'Europe/Kiev' AS order_date_kyiv_utc,
        order_date_kyiv AT TIME ZONE 'UTC' AS order_date_utc,
        order_date_kyiv AT TIME ZONE 'America/New_York' AS order_date_new_york,
        -- Разница в днях между датой возврата и датой продажи
        EXTRACT(DAY FROM (return_date_kyiv - order_date_kyiv)) AS days_between_return_and_order
    FROM cleaned_data
    GROUP BY 
        "Campaign Name", "Product Name", "Country", "Source", 
        return_date_kyiv, order_date_kyiv
)

SELECT
    "Campaign Name",
    "Product Name",
    "Country",
    "Source",
    "sales_agent_names",
    (COALESCE("Total Amount ($)", 0) + COALESCE("Total Rebill Amount", 0) - COALESCE("Returned Amount ($)", 0)) AS "Total Revenue",
    "Total Rebill Amount" AS "Rebill Revenue",
    "Number Of Rebills",
    "Discount Amount ($)",
    "Returned Amount ($)",
    return_date_kyiv_utc AS "Return Date Kyiv",
    return_date_utc AS "Return Date UTC",
    return_date_new_york AS "Return Date New York",
    order_date_kyiv_utc AS "Order Date Kyiv",
    order_date_utc AS "Order Date UTC",
    order_date_new_york AS "Order Date New York",
    days_between_return_and_order AS "Days Between Return and Order"
FROM aggregated_data
