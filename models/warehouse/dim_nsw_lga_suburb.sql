{{
    config(
        unique_key='suburb_id'
    )
}}

select * from {{ ref('nsw_lga_suburb_stg') }}