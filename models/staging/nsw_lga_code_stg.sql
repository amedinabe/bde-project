{{
    config(
        unique_key='lga_code'
    )
}}

SELECT 
    0 AS lga_code,
    'overseas' AS lga_name

UNION ALL

SELECT 
lga_code,
LOWER(lga_name) AS lga_name
FROM "postgres"."raw"."nsw_lga_code"

