CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_list_yearly_seasonality_unbiasing_table'] }}` AS
SELECT
    fk_sgplaces,
    fk_sgbrands AS fk_sgbrands_original,
    naics_code AS naics_code_manual
FROM
    (
        SELECT DISTINCT
            fk_sgplaces
        FROM
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_output_core_table'] }}`
    )
    INNER JOIN (
        SELECT
            fk_sgplaces
        FROM
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_class_business_table'] }}`
        WHERE
            tier_olvin > 2
            OR tier_sns > 2
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
            `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['yearly_seasonality_table'] }}`
    ) USING (naics_code);