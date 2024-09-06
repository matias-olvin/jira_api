CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['sgcentercameo_raw_table'] }}`
PARTITION BY
    local_date
CLUSTER BY
    fk_sgcenters AS
WITH
    final_table AS (
        SELECT
            fk_sgcenters,
            local_date,
            CAMEO_USA,
            SUM(weighted_visit_score) AS weighted_visit_score
        FROM
            (
                SELECT
                    *
                FROM
                    `{{ var.value.env_project }}.{{ params['cameo_staging_table'] }}.{{ params['smoothed_transition_table'] }}`
                WHERE
                    fk_sgplaces IN (
                        SELECT DISTINCT
                            pid
                        FROM
                            `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['sgplaceraw_table'] }}`
                        WHERE
                            fk_sgcenters IS NOT NULL
                    )
            )
            JOIN (
                SELECT
                    pid AS fk_sgplaces,
                    fk_sgcenters,
                    IFNULL(opening_date, DATE("2019-01-01")) AS opening_date,
                    IFNULL(
                        closing_date,
                        DATE(DATE_TRUNC("2024-03-01", MONTH))
                    ) AS closing_date
                FROM
                    `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['sgplaceraw_table'] }}`
                WHERE
                    fk_sgcenters IS NOT NULL
            ) USING (fk_sgplaces)
        WHERE
            local_date >= opening_date
            AND local_date <= closing_date
        GROUP BY
            fk_sgcenters,
            local_date,
            CAMEO_USA
    ),
    reference_dates AS (
        SELECT
            local_date,
            fk_sgcenters
        FROM
            UNNEST (
                GENERATE_DATE_ARRAY(
                    DATE('2019-01-01'),
                    DATE_SUB(
                        DATE_TRUNC(DATE_ADD("2024-03-01", INTERVAL 1 YEAR), YEAR),
                        INTERVAL 1 MONTH
                    ),
                    INTERVAL 1 MONTH
                )
            ) AS local_date
            CROSS JOIN (
                SELECT DISTINCT
                    pid AS fk_sgcenters
                FROM
                    `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['SGCenterRaw_table'] }}`
            )
    )
{% raw %}
SELECT
    fk_sgcenters,
    local_date,
    CONCAT(
        "[",
        REPLACE(
            REPLACE(
                REPLACE(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(
                                            REPLACE(
                                                TO_JSON_STRING(
                                                    ARRAY_AGG(
                                                        cameo_scores
                                                        ORDER BY
                                                            local_date_org
                                                    )
                                                ),
                                                "\\",
                                                ""
                                            ),
                                            "]",
                                            "}"
                                        ),
                                        "[",
                                        "{"
                                    ),
                                    '","',
                                    ","
                                ),
                                '{"{',
                                "{{"
                            ),
                            '}"}',
                            "}}"
                        ),
                        "}}",
                        "}"
                    ),
                    "}}",
                    "}"
                ),
                "{{",
                "{"
            ),
            "{{",
            "{"
        ),
        "]"
    ) AS cameo_scores
FROM
    (
        SELECT
            local_date AS local_date_org,
            DATE_TRUNC(local_date, YEAR) AS local_date,
            fk_sgcenters,
            IFNULL(cameo_scores, "{}") AS cameo_scores
        FROM
            (
                SELECT
                    local_date,
                    fk_sgcenters,
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                TO_JSON_STRING(
                                    ARRAY_AGG(
                                        STRUCT (
                                            CAST(CAMEO_USA AS STRING) AS CAMEO_USA,
                                            weighted_visit_score AS visit_score
                                        )
                                    )
                                ),
                                ',"visit_score"',
                                ''
                            ),
                            '"CAMEO_USA":',
                            ''
                        ),
                        '},{',
                        ','
                    ) AS cameo_scores
                FROM
                    final_table
                WHERE
                    weighted_visit_score > 0
                GROUP BY
                    local_date,
                    fk_sgcenters
            )
            RIGHT JOIN reference_dates USING (local_date, fk_sgcenters)
    )
GROUP BY
    fk_sgcenters,
    local_date
{% endraw %}