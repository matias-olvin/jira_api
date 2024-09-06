CREATE OR REPLACE TABLE
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['trade_cells_table'] }}_{{ source }}_{{ level }}` AS
SELECT s2_token, ROUND(visit_score/ SUM(visit_score) OVER () *100, 2) AS {{ source }}_percentage FROM
(SELECT {{ source }}_s2_token AS s2_token, SUM(visit_score) AS visit_score FROM
(SELECT
TO_HEX(
              CAST(
                (
                  -- We want teh final result in hexadecimal
                  SELECT
                    STRING_AGG(
                      CAST(
                        S2_CELLIDFROMPOINT({{ source }}_point, {{ level }}) >> bit & 0x1 AS STRING
                      ),
                      ''
                      ORDER BY
                        bit DESC
                    ) -- S2_CELLIDFROMPOINT(lat_lon_point, 14) gices an integer, convert to binary
                  FROM
                    UNNEST(GENERATE_ARRAY(0, 63)) AS bit -- The standard is 64-bit long binary encoding
                ) AS BYTES FORMAT "BASE2" -- Tell BQ it is a binary string
              )
            ) AS {{ source }}_s2_token,
visit_score
FROM
`{{ var.value.storage_project_id }}.{{ dag_run.conf['site_name'] }}.{{ params['visitors_homes_table'] }}`
WHERE {{ source }}_point IS NOT NULL)
GROUP BY {{ source }}_s2_token)
ORDER BY {{ source }}_percentage DESC