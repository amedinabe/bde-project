{% snapshot property_snapshot %}

{{ config(
  target_schema='raw',
  unique_key='property_id',
  strategy="timestamp",
  updated_at="scraped_date"
) }}

SELECT DISTINCT
	MD5(property_type) AS property_id,
	property_type,
	TO_DATE(scraped_date, 'YYYY-MM-DD') AS scraped_date
FROM {{ source('raw', 'listings') }}

{% endsnapshot %}