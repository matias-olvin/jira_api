BEGIN
CREATE TEMP TABLE
    joining_and_adding_factor AS
SELECT
    fk_sgplaces,
    a.fk_sgbrands,
    local_date,
    visits AS visits_input,
    visits * IFNULL(factor, 1) AS visits_output
FROM
    (
        SELECT
            *
        FROM
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_output_core_table'] }}`
            LEFT JOIN (
                SELECT
                    fk_sgplaces,
                    fk_sgbrands_original AS fk_sgbrands
                FROM
                    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_list_yearly_seasonality_unbiasing_table'] }}`
                UNION ALL
                SELECT pid AS fk_sgplaces, fk_sgbrands
                FROM
                  `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
                WHERE fk_sgbrands IN(
                    SELECT DISTINCT fk_sgbrands
                    FROM
                      `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['yearly_seasonality_brand_table'] }}`
                )
                  AND pid NOT IN(
                    SELECT fk_sgplaces
                    FROM
                      `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_list_yearly_seasonality_unbiasing_table'] }}`
                )
            ) USING (fk_sgplaces)
    ) a
    LEFT JOIN `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['yearly_seasonality_adjustment_brand_ratios_table'] }}` b ON EXTRACT(
        DAYOFYEAR
        FROM
            local_date
    ) - CAST(
        MOD(
            EXTRACT(
                YEAR
                FROM
                    local_date
            ),
            4
        ) = 0
        AND EXTRACT(
            MONTH
            FROM
                local_date
        ) > 2 AS INT64
    ) = b.day_of_year
    AND a.fk_sgbrands = b.fk_sgbrands;

END;

BEGIN
CREATE TEMP TABLE
    metric_1 AS (
        SELECT
            COUNT(*) AS new_count
        FROM
            joining_and_adding_factor
    );

END;

BEGIN
CREATE TEMP TABLE
    metric_2 AS (
        SELECT
            COUNT(*) AS old_count
        FROM
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_output_core_table'] }}`
    );

END;

BEGIN
CREATE TEMP TABLE
    int_table_ AS (
        SELECT DISTINCT
            fk_sgbrands_original AS fk_sgbrands
        FROM
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_list_yearly_seasonality_unbiasing_table'] }}`
    );

END;

BEGIN
CREATE TEMP TABLE
    metric_3 AS
SELECT
    CAST(SUM(visits_input) AS INT64) AS sum_visits_input,
    CAST(SUM(visits_output) AS INT64) AS sum_visits_output
FROM
    joining_and_adding_factor
WHERE
    fk_sgbrands IS NULL;

END;

CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_year_season_corrected_output_table'] }}` AS
WITH
    tests AS (
        SELECT
            *
        FROM
            joining_and_adding_factor,
            metric_1,
            metric_2,
            metric_3
    )
SELECT
    fk_sgplaces,
    local_date,
    visits_output AS visits
FROM
    tests
WHERE
    IF(
        (new_count = old_count)
        AND (sum_visits_input = sum_visits_output),
        TRUE,
        ERROR(
            FORMAT(
                "new_count  %d <> old_count %d, or  sum_visits_input %d <> sum_visits_output %d",
                new_count,
                old_count,
                sum_visits_input,
                sum_visits_output
            )
        )
    )