CREATE OR REPLACE TABLE
    `{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{ env }}.{{ params['model_output_table'] }}`
PARTITION BY
    local_date
CLUSTER BY
    fk_sgplaces AS
WITH
    model_input_table AS (
        SELECT
            fk_sgplaces,
            y,
            ds AS local_date
        FROM
            `{{ var.value.sns_project }}.{{ params['visits_estimation_dataset'] }}_{{ step }}{{ env }}.{{ params['model_input_table'] }}`
    ),
    adding_brands AS (
        SELECT
            *
        FROM
            model_input_table
            INNER JOIN (
                SELECT
                    pid AS fk_sgplaces,
                    fk_sgbrands
                FROM
                    `{{ var.value.env_project }}.{{ params['accessible_by_sns_dataset'] }}.v-{{ params['postgres_dataset'] }}-{{ params['sgplaceraw_table'] }}`
            ) USING (fk_sgplaces)
    ),
    adding_seasonality AS (
        SELECT
            fk_sgplaces,
            local_date,
            AVG(y) OVER (
                PARTITION BY
                    EXTRACT(
                        DAYOFWEEK
                        FROM
                            local_date
                    ),
                    fk_sgplaces
            ) AS poi_component,
            AVG(y) OVER (
                PARTITION BY
                    EXTRACT(
                        DAYOFWEEK
                        FROM
                            local_date
                    ),
                    fk_sgbrands
            ) AS brand_component
        FROM
            adding_brands
    ),
    scaling_seasonality AS (
        SELECT
            fk_sgplaces,
            local_date,
            poi_component / NULLIF(
                AVG(poi_component) OVER (
                    PARTITION BY
                        fk_sgplaces
                ),
                0
            ) AS poi_component_scaled,
            brand_component / NULLIF(
                AVG(brand_component) OVER (
                    PARTITION BY
                        fk_sgplaces
                ),
                0
            ) AS brand_component_scaled
        FROM
            adding_seasonality
    ),
    final_table AS (
        SELECT
            fk_sgplaces,
            local_date,
            0.8 * poi_component_scaled + 0.2 * brand_component_scaled AS visits
        FROM
            scaling_seasonality
    )
SELECT
    *
FROM
    final_table
WHERE
    IF(
        (
            SELECT
                COUNT(*)
            FROM
                model_input_table
            WHERE
                fk_sgplaces NOT LIKE '%_gt'
        ) = (
            SELECT
                COUNT(*)
            FROM
                final_table
        ),
        IF(
            (
                SELECT
                    COUNT(*)
                FROM
                    final_table
                WHERE
                    visits IS NULL
            ) = 0,
            TRUE,
            ERROR("Model output contains nulls in visits column")
        ),
        ERROR(
            "Model input length does not match output length."
        )
    );