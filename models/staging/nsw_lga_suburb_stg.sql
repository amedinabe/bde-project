{{
    config(
        unique_key='suburb_id'
    )
}}

WITH 
source AS (

    SELECT 
    LOWER(lga_name) AS lga_name, 
    LOWER(suburb_name) AS suburb_name , 
    ROW_NUMBER() OVER (ORDER BY suburb_name) AS suburb_id
    FROM "postgres"."raw"."nsw_lga_suburb"

),

code AS (

    SELECT lga_code, LOWER(lga_name) AS lga_name
    FROM "postgres"."raw"."nsw_lga_code"

)
SELECT 
	0 AS suburb_id, 
	'overseas' AS suburb_name,
	0 AS lga_code
    
UNION ALL

SELECT source.suburb_id, source.suburb_name, code.lga_code
FROM source
LEFT JOIN code ON source.lga_name = code.lga_name