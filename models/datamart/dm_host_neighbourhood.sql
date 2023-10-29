SELECT
	lga_codes.lga_name AS host_neighbourhood_lga,
	TO_CHAR(TO_DATE(EXTRACT(YEAR FROM scraped_date) || '-' || EXTRACT(MONTH FROM scraped_date), 'YYYY-MM'), 'Mon/YYYY') AS month_year,
	COUNT(DISTINCT host_id) AS distinct_hosts,
	SUM(price * (30 - availability_30)) AS estimated_revenue,
	SUM(price * (30 - availability_30)) / COUNT(DISTINCT host_id) AS revenue_per_host
FROM {{ ref('facts_listings') }} listings
JOIN {{ ref('dim_nsw_lga_code') }} lga_codes ON listings.host_lga_code = lga_codes.lga_code
WHERE has_availability = 't'
GROUP BY host_neighbourhood_lga, EXTRACT(YEAR FROM scraped_date), EXTRACT(MONTH FROM scraped_date)