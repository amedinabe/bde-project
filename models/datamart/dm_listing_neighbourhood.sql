WITH active_listings AS (
    SELECT
        listing_neighbourhood,
        EXTRACT(YEAR FROM scraped_date) AS year,
        EXTRACT(MONTH FROM scraped_date) AS month,
        COUNT(*) AS total_active_listings,
        AVG(price) AS avg_price_active,
        MIN(price) AS min_price_active,
        MAX(price) AS max_price_active,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price_active,
        AVG(review_scores_rating) AS avg_review_scores_active,
        SUM(30 - availability_30) AS total_stays_active,
        AVG((30 - availability_30) * price) AS avg_estimated_revenue_active
    FROM
        {{ ref('facts_listings') }}
    WHERE
        has_availability = 't'
    GROUP BY
        listing_neighbourhood, year, month
),
all_listings AS (
    SELECT
        listing_neighbourhood,
        EXTRACT(YEAR FROM scraped_date) AS year,
        EXTRACT(MONTH FROM scraped_date) AS month,
        COUNT(DISTINCT host_id) AS distinct_hosts,
	 	COUNT(DISTINCT CASE WHEN host_is_superhost = 't' THEN host_id END) AS distinct_hosts_superhost,
        COUNT(*) AS total_listings
    FROM
        {{ ref('facts_listings') }}
    GROUP BY
        listing_neighbourhood, year, month
)
SELECT
    al.listing_neighbourhood,
    TO_CHAR(TO_DATE(al.year || '-' || al.month, 'YYYY-MM'), 'YYYY/MM') AS month_year,
    --total_active_listings AS total_active_listings,
    --total_listings AS total_listings,
    (total_active_listings * 100.0 / total_listings) AS active_listing_rate,
    min_price_active,
    max_price_active,
    median_price_active,
	avg_price_active,
	distinct_hosts,
	--distinct_hosts_superhost
	(distinct_hosts_superhost * 100.0 / distinct_hosts) AS superhost_rate,
    avg_review_scores_active,
    (total_active_listings - lag(total_active_listings) OVER (PARTITION BY al.listing_neighbourhood ORDER BY al.year, al.month)) * 100.0 / lag(total_active_listings) OVER (PARTITION BY al.listing_neighbourhood ORDER BY al.year, al.month) AS active_listings_pct_change,
    CASE 
	    WHEN lag(total_listings - total_active_listings) OVER (PARTITION BY al.listing_neighbourhood ORDER BY al.year, al.month) = 0 THEN NULL 
	    ELSE ((total_listings - total_active_listings) - lag(total_listings - total_active_listings) OVER (PARTITION BY al.listing_neighbourhood ORDER BY al.year, al.month)) * 100.0 / lag(total_listings - total_active_listings) OVER (PARTITION BY al.listing_neighbourhood ORDER BY al.year, al.month) 
	    END AS inactive_listings_pct_change,
    total_stays_active,
	avg_estimated_revenue_active
FROM
    active_listings al
JOIN
    all_listings al2 ON al.listing_neighbourhood = al2.listing_neighbourhood AND al.year = al2.year AND al.month = al2.month
ORDER BY
    al.listing_neighbourhood, al.year, al.month


