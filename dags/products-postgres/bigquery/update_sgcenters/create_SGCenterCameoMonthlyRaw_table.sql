CREATE OR REPLACE TABLE
    `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['sgcentercameomonthtly_raw'] }}`
PARTITION BY
    local_date
CLUSTER BY
    fk_sgcenters AS
SELECT
    fk_sgcenters,
    DATE_ADD(local_date, INTERVAL ROW_NUMBER MONTH) AS local_date,
    {% raw %} CONCAT('{', cameo_month, '}') {% endraw %} AS cameo_scores
FROM
    `{{ var.value.env_project }}.{{ postgres_dataset }}.{{ params['sgcentercameo_raw_table'] }}`
    {% raw %}
    CROSS JOIN UNNEST (
        SPLIT(
            REPLACE(REPLACE(cameo_scores, '[{', ''), '}]', ''),
            '},{'
        )
    ) AS cameo_month
WITH
OFFSET
    AS ROW_NUMBER
{% endraw %}