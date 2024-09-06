CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.smc_{{ params['category_names_table'] }}`
AS
SELECT
REPLACE(ARRAY_AGG(sub_category
    ORDER BY
    number_places)[
OFFSET
    (0)], "Other ", "") AS name,
olvin_category
FROM (
SELECT
    COUNT(*) AS number_places,
    places.sub_category,
    olvin_category
FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}` AS places
JOIN
    `{{ var.value.env_project }}.{{ params['sg_base_tables_dataset'] }}.{{ params['naics_code_subcategories_table'] }}`
USING
    (naics_code)
GROUP BY
    sub_category,
    olvin_category)
GROUP BY
olvin_category
ORDER BY
name