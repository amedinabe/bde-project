{% snapshot host_snapshot %}

{{ config(
  target_schema='raw',
  unique_key='host_id',
  strategy="timestamp",
  updated_at="scraped_date"
) }}

SELECT DISTINCT
  host_id, 
  TO_DATE(scraped_date, 'YYYY-MM-DD') AS scraped_date, 
  host_name, 
  host_since, 
  host_is_superhost, 
  host_neighbourhood 
FROM {{ source('raw', 'listings') }}

{% endsnapshot %}