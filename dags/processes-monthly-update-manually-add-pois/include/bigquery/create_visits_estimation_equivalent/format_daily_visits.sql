CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{  params['sgplacedailyvisitsraw_table'] }}`
PARTITION BY
    local_date
CLUSTER BY
    fk_sgplaces AS
WITH
    adjustments_output AS (
        SELECT
            fk_sgplaces,
            local_date,
            CAST(visits AS INT64) AS visits
        FROM
            `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['daily_visits_table'] }}`
        WHERE
            local_date > '2018-12-31'
    ),
    -- picking observed or estimated based on activity
    daily_visits_array AS (
        SELECT
            fk_sgplaces,
            local_date,
            TO_JSON_STRING(
                ARRAY_AGG(
                    IFNULL(visits, 0)
                    ORDER BY
                        local_date_sort
                )
            ) AS visits
        FROM
            (
                SELECT
                    fk_sgplaces,
                    DATE_TRUNC(local_date, MONTH) AS local_date,
                    local_date AS local_date_sort,
                    visits
                FROM
                    adjustments_output
            )
        GROUP BY
            fk_sgplaces,
            local_date
    ),
    tests AS (
        SELECT
            *
        FROM
            daily_visits_array,
            (
                SELECT
                    COUNT(*) AS count_distinct
                FROM
                    (
                        SELECT DISTINCT
                            fk_sgplaces,
                            local_date
                        FROM
                            daily_visits_array
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
                            daily_visits_array
                    )
            ),
            (
                SELECT
                    COUNT(*) count_less_28
                FROM
                    (
                        SELECT
                            LENGTH(visits) - LENGTH(REPLACE(visits, ',', '')) AS comma_count,
                            COUNT(*) AS ct
                        FROM
                            daily_visits_array
                        WHERE
                            local_date < CAST('{{ ds }}' AS DATE)
                        GROUP BY
                            1
                    )
                WHERE
                    comma_count < 27
            )
    )
SELECT
    * EXCEPT (count_distinct, count_, count_less_28)
FROM
    tests
WHERE
    IF(
        (count_distinct = count_)
        AND count_less_28 = 0,
        TRUE,
        ERROR(
            FORMAT(
                "count_distinct  %d <> count_ %d or count_less_28 >0 %d ",
                count_distinct,
                count_,
                count_less_28
            )
        )
    )