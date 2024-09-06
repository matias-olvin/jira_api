CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{  params['sgplace_hourly_all_visits_raw_table'] }}`
PARTITION BY
    local_date
CLUSTER BY
    fk_sgplaces AS
WITH
    adjustments_output_hourly AS (
        SELECT
            fk_sgplaces,
            local_date,
            local_hour,
            CAST(visits AS INT64) AS visits
        FROM
            `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['hourly_visits_table'] }}`
    ),
    hourly_visits_array AS (
        SELECT
            fk_sgplaces,
            local_date,
            TO_JSON_STRING(
                ARRAY_AGG(
                    IFNULL(visits, 0)
                    ORDER BY
                        local_hour
                )
            ) AS visits
        FROM
            adjustments_output_hourly
        GROUP BY
            fk_sgplaces,
            local_date
    ),
    tests AS (
        SELECT
            *
        FROM
            hourly_visits_array,
            (
                SELECT
                    COUNT(*) AS count_distinct
                FROM
                    (
                        SELECT DISTINCT
                            fk_sgplaces,
                            local_date
                        FROM
                            hourly_visits_array
                    )
            ),
            (
                SELECT
                    COUNT(*) AS count_
                FROM
                    (
                        SELECT
                            fk_sgplaces,
                            local_date
                        FROM
                            hourly_visits_array
                    )
            ),
            (
                SELECT
                    COUNT(*) count_no_24
                FROM
                    (
                        SELECT
                            LENGTH(visits) - LENGTH(REPLACE(visits, ',', '')) AS comma_count,
                            COUNT(*) AS ct
                        FROM
                            hourly_visits_array
                        WHERE
                            local_date < CAST('{{ ds }}' AS DATE)
                        GROUP BY
                            1
                    )
                WHERE
                    comma_count <> 23
            )
    )
SELECT
    * EXCEPT (count_distinct, count_, count_no_24)
FROM
    tests
WHERE
    IF(
        (count_distinct = count_)
        AND count_no_24 = 0,
        TRUE,
        ERROR(
            FORMAT(
                "count_distinct  %d <> count_ %d or count_no_24 <>0 : %d ",
                count_distinct,
                count_,
                count_no_24
            )
        )
    )