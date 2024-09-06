CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_factor_places_table'] }}`
AS

WITH
factor_per_tier_id AS (
    SELECT tier_id, identifier, AVG(factor) AS factor
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_factor_tier_table'] }}`
    GROUP BY tier_id, identifier
),

manual_factors_brand AS(
    SELECT fk_sgplaces, identifier, factor
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_class_table'] }}`
    INNER JOIN
        `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['events_manual_impact_brand_table'] }}`
        ON tier_id_olvin = fk_sgbrands
    WHERE tier_sns > 1 -- Applied if there's no ground truth of that brand
),

manual_factors_brand_cat_based AS(
    SELECT fk_sgplaces, identifier, factor
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_class_table'] }}`
    INNER JOIN
        `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
        ON tier_id_olvin = pid
    INNER JOIN
        `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`
        USING(naics_code)
    INNER JOIN
        `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['events_manual_impact_olvin_category_table'] }}`
        USING(olvin_category)
    WHERE tier_sns > 2  -- Applied if there's no ground truth of that category (~naics code)
),

manual_factors_category AS(
    SELECT fk_sgplaces, identifier, factor
    FROM
        `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_class_table'] }}`
    INNER JOIN
        `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['events_manual_impact_olvin_category_table'] }}`
        ON tier_id_olvin = olvin_category
    WHERE tier_sns > 2  -- Applied if there's no ground truth of that category (~naics code)
)

SELECT a.fk_sgplaces, c.local_date, IFNULL(manual_factors.factor,IFNULL(gt_factors.factor, d.factor)) AS factor
FROM
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_class_table'] }}` a
INNER JOIN
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['events_holidays_poi_table'] }}` c
    USING(fk_sgplaces)
INNER JOIN
    factor_per_tier_id d
    ON tier_id_sns = tier_id AND c.identifier = d.identifier
LEFT JOIN
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['event_factor_tier_table'] }}` gt_factors
    ON tier_id_sns = gt_factors.tier_id AND c.local_date = gt_factors.local_date
LEFT JOIN(
    SELECT *
    FROM manual_factors_brand
    UNION ALL
    SELECT *
    FROM manual_factors_brand_cat_based
    UNION ALL
    SELECT *
    FROM manual_factors_category
) manual_factors
ON a.fk_sgplaces = manual_factors.fk_sgplaces AND c.identifier = manual_factors.identifier