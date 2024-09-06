CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplacecameomonthlyraw_table'] }}`
PARTITION BY
    local_date
CLUSTER BY
    fk_sgplaces AS
SELECT
    fk_sgplaces,
    DATE_ADD(local_date, INTERVAL ROW_NUMBER MONTH) AS local_date,
    CONCAT('{', cameo_month, '}') AS cameo_scores
FROM
    `{{ var.value.env_project }}.{{ params['manually_add_pois_dataset'] }}.{{ params['sgplacecameoraw_table'] }}`
    CROSS JOIN UNNEST (
        SPLIT(
            REPLACE(REPLACE(cameo_scores, '[{', ''), '}]', ''),
            '},{'
        )
    ) AS cameo_month
WITH
OFFSET
    AS ROW_NUMBER;
