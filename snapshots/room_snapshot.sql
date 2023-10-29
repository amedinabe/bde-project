{% snapshot room_snapshot %}

{{ config(
  target_schema='raw',
  unique_key='room_id',
  strategy="timestamp",
  updated_at="scraped_date"
) }}

SELECT DISTINCT
	MD5(room_type) AS room_id,
	room_type,
	TO_DATE(scraped_date, 'YYYY-MM-DD') AS scraped_date
FROM {{ source('raw', 'listings') }} 

{% endsnapshot %}