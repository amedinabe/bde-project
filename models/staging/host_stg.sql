{{
    config(
        unique_key='row_num_host'
    )
}}

WITH
history AS (
SELECT
    ROW_NUMBER() OVER (ORDER BY scraped_date) AS row_num_host,
    host_id,
    CASE
        WHEN LOWER(host_name) = 'nan' THEN 'unavailable'
        ELSE host_name 
        END AS host_name, 
    CASE 
        WHEN host_since = 'NaN' THEN TO_DATE('01/01/1900' , 'DD/MM/YYYY') 
        ELSE TO_DATE(host_since, 'DD/MM/YYYY') 
        END AS host_since,
    host_is_superhost, 
    CASE
        WHEN LOWER(host_neighbourhood) = 'nan' THEN 'unavailable'
        ELSE host_neighbourhood 
        END AS host_neighbourhood,
    scraped_date,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('host_snapshot') }}
),
unknown AS (
    SELECT 
        0 AS row_num_host,
        0 AS host_id,
        'unavailable' AS host_name,
        '1900-01-01'::DATE AS host_since,
        'unavailable' AS host_is_superhost,
        'unavailable' AS host_neighbourhood,
        '1900-01-01'::DATE AS scraped_date,
        '1900-01-01'::DATE AS dbt_valid_from,
        NULL:: DATE AS dbt_valid_to
)
SELECT * FROM unknown
UNION ALL
SELECT * FROM history