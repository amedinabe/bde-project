WITH active_listings AS (
    SELECT
        property_type,
        room_type,
        accommodates,
        EXTRACT(YEAR FROM scraped_date) AS year,
        EXTRACT(MONTH FROM scraped_date) AS month,
        COUNT(*) AS total_active_listings,
        MIN(price) AS min_price_active,
        MAX(price) AS max_price_active,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price_active,
		AVG(price) AS avg_price_active,
        AVG(review_scores_rating) AS avg_rating_active,
        SUM(30 - availability_30) AS total_stays_active,
		AVG((30 - availability_30) * price) AS avg_estimated_revenue_active
    FROM {{ ref('facts_listings') }}
    WHERE has_availability = 't'
    GROUP BY property_type, room_type, accommodates, year, month
),
total_listings AS (
    SELECT
        property_type,
        room_type,
        accommodates,
        EXTRACT(YEAR FROM scraped_date) AS year,
        EXTRACT(MONTH FROM scraped_date) AS month,
        COUNT(DISTINCT host_id) AS distinct_hosts,
        COUNT(DISTINCT CASE WHEN host_is_superhost = 't' THEN host_id END) AS distinct_hosts_superhost,
        COUNT(*) AS total_listings
    FROM {{ ref('facts_listings') }}
    GROUP BY property_type, room_type, accommodates, year, month
)
SELECT
    al.property_type,
    al.room_type,
    al.accommodates,
	TO_CHAR(TO_DATE(al.year || '-' || al.month, 'YYYY-MM'), 'YYYY/MM') AS month_year,
    --al.total_active_listings AS total_active_listings,
	--al2.total_listings,
	(al.total_active_listings * 100.0 / al2.total_listings) AS active_listing_rate,
    al.min_price_active,
    al.max_price_active,
    al.median_price_active,
	al.avg_price_active,
	--al.distinct_hosts_superhost
    al2.distinct_hosts,
    (al2.distinct_hosts_superhost * 100.0 / al2.distinct_hosts) AS superhost_rate,
	al.avg_rating_active,
	(total_active_listings - lag(total_active_listings) OVER (PARTITION BY al.property_type, al.room_type, al.accommodates  ORDER BY al.year, al.month)) * 100.0 / lag(al.total_active_listings) OVER (PARTITION BY al.property_type, al.room_type, al.accommodates  ORDER BY al.year, al.month) AS active_listings_pct_change,
    CASE 
	    WHEN lag(total_listings - total_active_listings) OVER (PARTITION BY al.property_type, al.room_type, al.accommodates ORDER BY al.year, al.month) = 0 THEN NULL 
	    ELSE ((total_listings - total_active_listings) - lag(total_listings - total_active_listings) OVER (PARTITION BY al.property_type, al.room_type, al.accommodates ORDER BY al.year, al.month)) * 100.0 / lag(total_listings - total_active_listings) OVER (PARTITION BY al.property_type, al.room_type, al.accommodates ORDER BY al.year, al.month) 
	    END AS inactive_listings_pct_change,
    al.total_stays_active,
   	al.avg_estimated_revenue_active
FROM active_listings al 
JOIN total_listings al2 ON al.property_type = al2.property_type AND al.room_type = al2.room_type AND al.accommodates = al2.accommodates AND al.year = al2.year AND al.month = al2.month
