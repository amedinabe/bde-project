{{
    config(
        unique_key='row_num_listing'
    )
}}

WITH
check_dimensions AS (
SELECT
	row_num_listing,
	listing_id,
	scraped_date,
	CASE 
		WHEN host_id IN (SELECT DISTINCT host_id FROM {{ ref('host_stg') }} ) THEN host_id 
		ELSE 0 
		END AS host_id,
	host_is_superhost,
	host_neighbourhood,
	listing_neighbourhood,
	CASE 
		WHEN property_type IN (SELECT DISTINCT property_type FROM {{ ref('property_stg') }} ) THEN property_type 
		ELSE 'unavailable' 
	END AS property_type,
	CASE 
		WHEN room_type IN (SELECT DISTINCT room_type FROM {{ ref('room_stg') }} ) THEN room_type 
		ELSE 'unavailable' 
	END AS room_type,
	accommodates, 
	price, 
	has_availability, 
	availability_30, 
	number_of_reviews, 
	review_scores_rating
FROM {{ ref('listings_stg') }}
)

SELECT
	a.row_num_listing,
	a.listing_id,
	a.scraped_date,
	a.host_id,
	b.host_name,
	b.host_since,
	a.host_is_superhost,
	a.host_neighbourhood,
	e.lga_code AS host_lga_code,
	a.listing_neighbourhood,
	f.lga_code AS listing_lga_code,
	c.row_num_property,
	c.property_id,
	c.property_type,
	d.row_num_room,
	d.room_id,
	d.room_type,
	a.accommodates, 
	a.price, 
	a.has_availability, 
	a.availability_30, 
	a.number_of_reviews, 
	a.review_scores_rating
FROM check_dimensions a
LEFT JOIN {{ ref('host_stg') }} b 
	ON a.host_id = b.host_id
	AND a.scraped_date::timestamp >= b.dbt_valid_from::timestamp 
	AND a.scraped_date::timestamp < b.dbt_valid_to::timestamp
LEFT JOIN {{ ref('property_stg') }} c 
	ON a.property_type = c.property_type
	AND a.scraped_date::timestamp >= c.dbt_valid_from::timestamp 
	AND a.scraped_date::timestamp < c.dbt_valid_to::timestamp
LEFT JOIN {{ ref('room_stg') }} d 
	ON a.room_type = d.room_type
	AND a.scraped_date::timestamp >= d.dbt_valid_from::timestamp 
	AND a.scraped_date::timestamp < d.dbt_valid_to::timestamp
LEFT JOIN {{ ref('nsw_lga_suburb_stg') }} e 
	ON a.host_neighbourhood = e.suburb_name
LEFT JOIN {{ ref('nsw_lga_suburb_stg') }} f 
	ON a.listing_neighbourhood = f.suburb_name