CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
AS
-- obtain domains for branded pois from smc places_dynamic
WITH domains_of_each_poi AS (
  SELECT pid, fk_sgbrands, domains	
  FROM `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}`
  WHERE fk_sgbrands IS NOT NULL AND NOT CONTAINS_SUBSTR(fk_sgbrands, ',') AND domains NOT LIKE "[]"
),
-- explode domains array (ignore empty domains) so that 1 row explodes into the same number of rows as there are domains in the array
exploded_domains AS (
SELECT fk_sgbrands, JSON_EXTRACT_SCALAR(un_cleaned_domain, "$") AS domain
FROM domains_of_each_poi,
UNNEST(JSON_EXTRACT_ARRAY(domains)) AS un_cleaned_domain
),
-- group by brand, domain to obtain count
grouped_brand_and_domain_with_count AS (
SELECT fk_sgbrands, domain, COUNT(*) as _count
FROM exploded_domains
GROUP BY fk_sgbrands, domain
),
-- rank most common domain for that brand and pick the most seen
popularity_ranked AS (
  SELECT fk_sgbrands,
         domain, 
  ROW_NUMBER() OVER (PARTITION BY fk_sgbrands ORDER BY _count DESC) as rn
  FROM grouped_brand_and_domain_with_count
),
most_popular_domain_for_each_fk_sgbrands AS (
SELECT fk_sgbrands, domain
FROM popularity_ranked
WHERE rn = 1
)
-- join to sgbrandraw, leave domain column null if no domain exists for it
SELECT base.*, brands_domain.domain
FROM `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}` base
LEFT JOIN most_popular_domain_for_each_fk_sgbrands brands_domain ON base.pid=brands_domain.fk_sgbrands
