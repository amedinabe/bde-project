{{
    config(
        unique_key='lga_code'
    )
}}

select * from {{ ref('census_g02_stg') }}