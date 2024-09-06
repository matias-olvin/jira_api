BEGIN
CREATE TEMP TABLE
    factors_table AS
WITH
    brand_week_distribution AS (
        SELECT
            EXTRACT(
                DAYOFWEEK
                FROM
                    local_date
            ) AS dow,
            fk_sgbrands,
            AVG(visits) AS visits
        FROM
            (
                SELECT
                    local_date,
                    visits,
                    fk_sgplaces,
                FROM
                    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_year_season_corrected_output_table'] }}`
            )
            INNER JOIN (
                SELECT
                    fk_sgplaces,
                    fk_sgbrands_original AS fk_sgbrands
                FROM
                    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_list_weekly_seasonality_unbiasing_table'] }}`
            ) USING (fk_sgplaces)
        GROUP BY
            1,
            2
    ),
    naics_code_week_distribution AS (
        SELECT
            naics_code,
            day_of_week AS dow,
            value
        FROM
            `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['weekly_seasonality_table'] }}`,
            UNNEST (
                [
                    STRUCT (2 AS day_of_week, Monday AS value),
                    STRUCT (3, Tuesday),
                    STRUCT (4, Wednesday),
                    STRUCT (5, Thursday),
                    STRUCT (6, Friday),
                    STRUCT (7, Saturday),
                    STRUCT (1, Sunday)
                ]
            )
    ),
    adjust_brand_week_distribution AS (
        SELECT
            fk_sgbrands,
            day_of_week AS dow,
            value
        FROM
            `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['weekly_seasonality_brand_table'] }}`,
            UNNEST (
                [
                    STRUCT (2 AS day_of_week, Monday AS value),
                    STRUCT (3, Tuesday),
                    STRUCT (4, Wednesday),
                    STRUCT (5, Thursday),
                    STRUCT (6, Friday),
                    STRUCT (7, Saturday),
                    STRUCT (1, Sunday)
                ]
            )
    ),
    scaling_brand_week_distribution AS (
        SELECT
            * EXCEPT (visits),
            visits / AVG(visits) OVER (
                PARTITION BY
                    fk_sgbrands
            ) AS visits
        FROM
            brand_week_distribution
    ),
    scaling_naics_code_week_distribution AS (
        SELECT
            * EXCEPT (value),
            value / AVG(value) OVER (
                PARTITION BY
                    naics_code
            ) AS value
        FROM
            naics_code_week_distribution
    ),
    scaling_adjust_brand_week_distribution AS (
        SELECT
            * EXCEPT (value),
            value / AVG(value) OVER (
                PARTITION BY
                    fk_sgbrands
            ) AS value
        FROM
            adjust_brand_week_distribution
    ),
    mapping_table AS (
        SELECT DISTINCT
            fk_sgbrands_original AS fk_sgbrands,
            IFNULL(CAST(naics_code_manual AS STRING), fk_sgbrands_original) AS mapping_id
        FROM
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_list_weekly_seasonality_unbiasing_table'] }}`
    ),
    factors_table_before_tests AS (
        SELECT
            *,
            value / visits AS ratio
        FROM
            scaling_brand_week_distribution
            INNER JOIN mapping_table USING (fk_sgbrands)
            INNER JOIN(
              SELECT * EXCEPT(naics_code), CAST(naics_code AS STRING) AS mapping_id
              FROM scaling_naics_code_week_distribution
              UNION ALL
              SELECT * EXCEPT(fk_sgbrands), fk_sgbrands AS mapping_id
              FROM scaling_adjust_brand_week_distribution
              )
            USING (mapping_id, dow)
        ORDER BY
            fk_sgbrands,
            dow
    ),
    tests AS (
        SELECT
            *
        FROM
            factors_table_before_tests,
            (
                SELECT
                    COUNT(*) * 7 AS nb_brands_times_seven
                FROM
                    mapping_table
            ),
            (
                SELECT
                    COUNT(*) count_factors
                FROM
                    factors_table_before_tests
            )
    )
SELECT
    fk_sgbrands,
    dow,
    ratio AS week_adjustment_ratio
FROM
    tests
WHERE
    IF(
        (nb_brands_times_seven = count_factors)
        AND (CAST(ratio * 10000 AS INT64) > 0)
        AND (ratio IS NOT NULL),
        TRUE,
        ERROR(
            FORMAT(
                "nb_brands_times_seven  %d <> count_factors %d, or ratio is 0 or null (%d / 10000) ",
                nb_brands_times_seven,
                count_factors,
                CAST(ratio * 10000 AS INT64)
            )
        )
    );

END;

BEGIN
CREATE TEMP TABLE
    joining_and_adding_factor AS
SELECT
    fk_sgplaces,
    a.fk_sgbrands,
    local_date,
    visits AS visits_input,
    visits * IFNULL(week_adjustment_ratio, 1) AS visits_output
FROM
    (
        SELECT
            *
        FROM
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_year_season_corrected_output_table'] }}`
            LEFT JOIN (
                SELECT
                    fk_sgplaces,
                    fk_sgbrands_original AS fk_sgbrands
                FROM
                    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_list_weekly_seasonality_unbiasing_table'] }}`
            ) USING (fk_sgplaces)
    ) a
    LEFT JOIN factors_table b ON EXTRACT(
        DAYOFWEEK
        FROM
            local_date
    ) = b.dow
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
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_year_season_corrected_output_table'] }}`
    );

END;

BEGIN
CREATE TEMP TABLE
    int_table_ AS (
        SELECT DISTINCT
            fk_sgbrands_original AS fk_sgbrands
        FROM
            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_list_weekly_seasonality_unbiasing_table'] }}`
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
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_output_table'] }}`
PARTITION BY local_date
CLUSTER BY fk_sgplaces
AS
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