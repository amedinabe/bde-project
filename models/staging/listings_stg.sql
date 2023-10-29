{{
    config(
        unique_key='row_num_listing'
    )
}}

SELECT 
    ROW_NUMBER() OVER (ORDER BY scraped_date) AS row_num_listing,
    listing_id,  
    TO_DATE(scraped_date, 'YYYY-MM-DD') AS scraped_date, 
    host_id, 
    host_is_superhost,
    CASE 
        WHEN LOWER(host_neighbourhood) = 'nan' THEN 'unavailable' 
        ELSE LOWER(host_neighbourhood) 
        END AS host_neighbourhood,
    CASE 
        WHEN LOWER(listing_neighbourhood) = 'nan' THEN 'unavailable'
        WHEN LOWER(listing_neighbourhood) = 'cumberland' THEN 'cumberland reach'
        WHEN LOWER(listing_neighbourhood) = 'bayside' THEN 'mascot'
        WHEN LOWER(listing_neighbourhood) = 'canterbury-bankstown' THEN 'canterbury' 
        WHEN LOWER(listing_neighbourhood) = 'georges river' THEN 'hurstville'
        WHEN LOWER(listing_neighbourhood) = 'inner west' THEN 'burwood'
        WHEN LOWER(listing_neighbourhood) = 'northern beaches' THEN 'brookvale'
        WHEN LOWER(listing_neighbourhood) = 'sutherland shire' THEN 'miranda'
        WHEN LOWER(listing_neighbourhood) = 'the hills shire' THEN 'castle hill'
        ELSE LOWER(listing_neighbourhood) 
        END AS listing_neighbourhood,
    property_type, 
    room_type, 
    accommodates, 
    price, 
    has_availability, 
    availability_30, 
    number_of_reviews, 
    CASE 
        WHEN review_scores_rating = 'NaN' THEN NULL 
        ELSE review_scores_rating 
        END AS review_scores_rating
FROM {{ source('raw', 'listings') }} --"postgres"."raw"."listings"
WHERE 
    (room_type LIKE 'Entire home/apt' AND price BETWEEN 30 and 15000)
    OR (room_type = 'Private room' AND price BETWEEN 10 and 10000)
    OR (room_type = 'Shared room' AND price BETWEEN 10 and 10000)
    OR (room_type = 'Hotel room' AND price BETWEEN 20 and 15000)