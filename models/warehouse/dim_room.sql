{{
    config(
        unique_key='row_num_room'
    )
}}

SELECT 
    row_num_room,
    room_id, 
    room_type, 
    dbt_valid_from, 
    dbt_valid_to
FROM {{ ref('room_stg') }}