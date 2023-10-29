{{
    config(
        unique_key='row_num_property'
    )
}}

SELECT 
    row_num_property,
    property_id, 
    property_type, 
    dbt_valid_from, 
    dbt_valid_to
FROM {{ ref('property_stg') }}