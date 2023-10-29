{{
    config(
        unique_key='row_num_host'
    )
}}


SELECT
    row_num_host,
    host_id,
    host_name, 
    host_since,
    host_is_superhost, 
    host_neighbourhood,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('host_stg') }}

