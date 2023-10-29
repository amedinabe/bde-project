{{
    config(
        unique_key='row_num_property'
    )
}}

WITH
history AS (
SELECT
    ROW_NUMBER() OVER (ORDER BY scraped_date) AS row_num_property,
    property_id,
    CASE
        WHEN LOWER(property_type) = 'nan' THEN 'unavailable'
        ELSE property_type 
        END AS property_type, 
    scraped_date,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('property_snapshot') }}
),
unknown AS (
    SELECT 
        0 AS row_num_property,
        'unavailable' AS property_id,
        'unavailable' AS property_type,
        '1900-01-01'::DATE AS scraped_date,
        '1900-01-01'::DATE AS dbt_valid_from,
        NULL:: DATE AS dbt_valid_to
)
SELECT * FROM unknown
UNION ALL
SELECT * FROM history