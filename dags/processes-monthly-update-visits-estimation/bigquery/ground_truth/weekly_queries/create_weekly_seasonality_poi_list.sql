CREATE OR REPLACE TABLE `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_list_weekly_seasonality_unbiasing_table'] }}` AS

WITH naics_based AS(
    SELECT
        fk_sgplaces,
        fk_sgbrands AS fk_sgbrands_original,
        naics_code AS naics_code_manual
    FROM
        (
            SELECT DISTINCT
                fk_sgplaces
            FROM
                `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_year_season_corrected_output_table'] }}`
        )
        INNER JOIN (
            SELECT
                fk_sgplaces
            FROM
                `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_class_business_table'] }}`
            WHERE
                tier_olvin >= 2
                OR tier_sns >= 2
        ) USING (fk_sgplaces)
        INNER JOIN (
            SELECT
                pid AS fk_sgplaces,
                fk_sgbrands
            FROM
                `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['places_postgres_table'] }}`
        ) USING (fk_sgplaces)
        INNER JOIN (
            SELECT
                pid AS fk_sgbrands,
                naics_code
            FROM
                `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgbrandraw_table'] }}`
        ) USING (fk_sgbrands)
        INNER JOIN (
            SELECT DISTINCT
                naics_code
            FROM
                `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['weekly_seasonality_table'] }}`
        ) USING (naics_code)
),

brand_based AS(
SELECT
    fk_sgplaces,
    fk_sgbrands AS fk_sgbrands_original,
    null AS naics_code_manual
FROM
    (
        SELECT DISTINCT
            fk_sgplaces
        FROM
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_year_season_corrected_output_table'] }}`
    )
    INNER JOIN (
        SELECT
            fk_sgplaces
        FROM
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_class_business_table'] }}`
        WHERE
            tier_olvin > 1
            OR tier_sns > 1
    ) USING (fk_sgplaces)
    INNER JOIN (
        SELECT
            pid AS fk_sgplaces,
            fk_sgbrands
        FROM
            `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['places_postgres_table'] }}`
    ) USING (fk_sgplaces)
    INNER JOIN (
        SELECT DISTINCT
            fk_sgbrands
        FROM
            `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['weekly_seasonality_brand_table'] }}`
    ) USING (fk_sgbrands)
)

SELECT *
FROM naics_based
WHERE fk_sgplaces NOT IN(
   SELECT fk_sgplaces
   FROM brand_based
)

UNION ALL

SELECT *
FROM brand_based