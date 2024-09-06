CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sg_placehome_zipcode_yearly_table'] }}` AS
WITH
    ExtractedData AS (
        SELECT
            local_date,
            fk_sgplaces,
            JSON_EXTRACT_ARRAY(locations) AS location_data
        FROM
            `{{ var.value.env_project }}.{{ params['postgres_dataset'] }}.{{ params['sg_place_home_zipCode_table'] }}`
    ),
    UnnestedData AS (
        SELECT
            local_date,
            fk_sgplaces,
            location_item
        FROM
            ExtractedData,
            UNNEST (location_data) AS location_item
    ),
    your_unnested_table_name AS (
        SELECT
            local_date,
            fk_sgplaces,
            CAST(
                REGEXP_EXTRACT(CAST(location_item AS STRING), r':(.*?)}') AS FLOAT64
            ) AS percentage,
            REGEXP_EXTRACT(CAST(location_item AS STRING), r'"{1}(.*?)"{1}:') AS zip_code
        FROM
            UnnestedData
    ),
    TTM_table AS (
        SELECT DISTINCT
            original_table.fk_sgplaces,
            original_table.local_date,
            ttm_table.zip_code,
            ttm_table.percentage
        FROM
            your_unnested_table_name original_table
            INNER JOIN your_unnested_table_name ttm_table ON ttm_table.fk_sgplaces = original_table.fk_sgplaces
            AND ttm_table.local_date BETWEEN DATE_ADD(
                DATE_SUB(original_table.local_date, INTERVAL 1 YEAR),
                INTERVAL 1 MONTH
            ) AND original_table.local_date
    ),
    SummedData AS (
        SELECT
            fk_sgplaces,
            local_date,
            zip_code,
            SUM(percentage) AS summed_percentage
        FROM
            TTM_table
        WHERE
            EXTRACT(
                MONTH
                FROM
                    local_date
            ) IN (1, 4, 7, 10)
            AND local_date >= '2021-01-01'
        GROUP BY
            fk_sgplaces,
            zip_code,
            local_date
    ),
    normalized AS (
        SELECT
            fk_sgplaces,
            local_date,
            zip_code,
            (
                summed_percentage / SUM(summed_percentage) OVER (
                    PARTITION BY
                        fk_sgplaces,
                        local_date
                )
            ) * 100 AS normalized_percentage
        FROM
            SummedData
    ),
    AggregatedData AS (
        SELECT
            fk_sgplaces,
            local_date,
            STRING_AGG(
                CONCAT(
                    '{"',
                    CAST(zip_code AS STRING),
                    '":"',
                    CAST(normalized_percentage AS STRING),
                    '"}'
                ),
                ',' -- Delimiter
            ) AS combined_locations
        FROM
            normalized
        GROUP BY
            fk_sgplaces,
            local_date
    ),
    with_local_date AS (
        SELECT
            local_date,
            fk_sgplaces,
            CAST(CONCAT('[', combined_locations, ']') AS STRING) AS locations
        FROM
            AggregatedData
    )
SELECT
    *
FROM
    with_local_date;