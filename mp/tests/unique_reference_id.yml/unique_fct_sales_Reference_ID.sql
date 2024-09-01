-- tests/unique_reference_id.sql

WITH duplicate_records AS (
    SELECT
        "Reference ID",
        COUNT(*) AS n_records
    FROM {{ ref('fct_sales') }}
    WHERE "Reference ID" IS NOT NULL
    GROUP BY "Reference ID"
    HAVING COUNT(*) > 1
)

SELECT
    "Reference ID",
    COUNT(*) AS occurrences
FROM {{ ref('fct_sales') }}
WHERE "Reference ID" IN (SELECT "Reference ID" FROM duplicate_records)
GROUP BY 
    "Reference ID"
ORDER BY occurrences DESC
