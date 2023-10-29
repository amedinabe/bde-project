{{
    config(
        unique_key='row_num_room'
    )
}}

WITH
history AS (
SELECT
    ROW_NUMBER() OVER (ORDER BY scraped_date) AS row_num_room,
    room_id,
    CASE
        WHEN LOWER(room_type) = 'nan' THEN 'unavailable'
        ELSE room_type 
        END AS room_type, 
    scraped_date,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('room_snapshot') }}
),
unknown AS (
    SELECT 
        0 AS row_num_room,
        'unavailable' AS room_id,
        'unavailable' AS room_type,
        '1900-01-01'::DATE AS scraped_date,
        '1900-01-01'::DATE AS dbt_valid_from,
        NULL:: DATE AS dbt_valid_to
)
SELECT * FROM unknown
UNION ALL
SELECT * FROM history
