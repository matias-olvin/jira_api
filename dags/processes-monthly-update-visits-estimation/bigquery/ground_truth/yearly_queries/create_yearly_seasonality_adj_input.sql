CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['yearly_seasonality_adjustment_input_table'] }}` AS
WITH
    intermediate_naics_table AS (
        SELECT
               fk_sgbrands,
               local_date,
               fk_sgbrands_original_visits,
               naics_code_manual_visits AS manual_visits
        FROM
            (
                SELECT
                    fk_sgbrands_original AS fk_sgbrands,
                    naics_code_manual AS naics_code,
                    local_date,
                    AVG(visits) AS fk_sgbrands_original_visits
                FROM
                    `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_output_core_table'] }}`
                    INNER JOIN (
                        SELECT
                            fk_sgplaces,
                            fk_sgbrands_original,
                            naics_code_manual
                        FROM
                            `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['poi_list_yearly_seasonality_unbiasing_table'] }}`
                    ) USING (fk_sgplaces)
                GROUP BY
                    1,
                    2,
                    3
            )
            LEFT JOIN (
                SELECT
                    naics_code,
                    local_date,
                    yearly_seasonality_value AS naics_code_manual_visits
                FROM
                    `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['yearly_seasonality_table'] }}`
            ) USING (naics_code, local_date)
    ),

    intermediate_brands_table AS(
        SELECT
              fk_sgbrands,
              local_date,
              fk_sgbrands_original_visits,
              yearly_seasonality_value AS manual_visits
        FROM(
            SELECT
                  fk_sgbrands,
                  local_date,
                  AVG(visits) AS fk_sgbrands_original_visits
            FROM
              `{{ var.value.env_project }}.{{ params['visits_estimation_dataset'] }}.{{ params['ground_truth_output_core_table'] }}`
            INNER JOIN(
                SELECT pid AS fk_sgplaces, fk_sgbrands
                FROM
                  `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
                WHERE fk_sgbrands IN(
                    SELECT DISTINCT fk_sgbrands
                    FROM
                      `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['yearly_seasonality_brand_table'] }}`
                )
            )
            USING(fk_sgplaces)
            GROUP BY 1, 2
        )
        LEFT JOIN
          `{{ var.value.env_project }}.{{ params['static_features_dataset'] }}.{{ params['yearly_seasonality_brand_table'] }}`
        USING(fk_sgbrands, local_date)
    ),

    final_table AS(
        SELECT *
        FROM intermediate_naics_table
        WHERE fk_sgbrands NOT IN(
            SELECT DISTINCT fk_sgbrands
            FROM intermediate_brands_table
        )
        UNION ALL
        SELECT *
        FROM intermediate_brands_table
    ),

    tests AS (
        SELECT
            *,
            COUNT(fk_sgbrands_original_visits) OVER (
                PARTITION BY
                    fk_sgbrands
            ) AS fk_sgbrands_visits_nb,
            COUNT(manual_visits) OVER (
                PARTITION BY
                    fk_sgbrands
            ) AS yearly_seasonality_visits_nb,
            COUNT(*) OVER () AS rows_nb
        FROM
            final_table,
            (
                SELECT
                    COUNT(*) AS distinct_elements
                FROM
                    (
                        SELECT DISTINCT
                            fk_sgbrands,
                            local_date
                        FROM
                            final_table
                    )
            )
    )
SELECT
    * EXCEPT (
        fk_sgbrands_visits_nb,
        yearly_seasonality_visits_nb,
        rows_nb,
        distinct_elements
    )
FROM
    tests
WHERE
    IF(
        (fk_sgbrands_visits_nb > 1900)
        AND (yearly_seasonality_visits_nb = 365)
        AND (rows_nb = distinct_elements),
        TRUE,
        ERROR(
            FORMAT(
                "fk_sgbrands_visits_nb  %d <= 1900, or  yearly_seasonality_visits_nb %d <> 365, or rows_nb %d <> distinct_elements %d",
                fk_sgbrands_visits_nb,
                yearly_seasonality_visits_nb,
                rows_nb,
                distinct_elements
            )
        )
    )