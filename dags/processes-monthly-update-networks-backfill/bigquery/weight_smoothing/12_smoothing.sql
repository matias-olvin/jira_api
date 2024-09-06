with observed_connections_edges_table as (
    SELECT
        *
    FROM
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['second_deg_conn_table'] }}`
),
dates_nodes_reference AS (
    SELECT
local_date,
SUM(norm_value) AS date_factor
FROM
    (SELECT
        local_date,
        (
              0.5 + 0.5 * COS(DATE_DIFF(local_date, local_date_2, MONTH) * ACOS(-1) / 6)
            ) AS norm_value,
    FROM (
        SELECT
            GENERATE_DATE_ARRAY(MIN(local_date), DATE_ADD(MAX(local_date), INTERVAL 1 MONTH)) AS date_array
        FROM observed_connections_edges_table
    )
    CROSS JOIN UNNEST(date_array) AS local_date
    CROSS JOIN UNNEST(date_array) AS local_date_2)
    GROUP BY local_date
),
smoothed_weights AS (
    SELECT
    src_node, dst_node,
    local_date,
    weight/date_factor AS weight,
  FROM
    (
      SELECT
        src_node, dst_node,
        local_date,
        SUM(visit_score) AS weight
      FROM
        (
          SELECT
            CASE
              WHEN days_difference < 0 THEN DATE_SUB(local_date, INTERVAL - days_difference MONTH)
              WHEN days_difference > 0 THEN DATE_ADD(local_date, INTERVAL days_difference MONTH)
              ELSE local_date
            END AS local_date,
            src_node, dst_node,
            weight * (
              0.5 + 0.5 * COS(days_difference * ACOS(-1) / 6)
            ) AS visit_score,
          FROM
            (SELECT local_date, IFNULL(weight, 0) AS weight, src_node, dst_node FROM observed_connections_edges_table
            )
            CROSS JOIN UNNEST(
              GENERATE_ARRAY(
                - 6,
                6,
                1
              )
            ) AS days_difference
        )
        WHERE local_date >= "2021-09-01" AND local_date < DATE_ADD("{{ ds.format('%Y-%m-01') }}", INTERVAL 1 MONTH)
      GROUP BY
        src_node, dst_node,
        local_date
)
JOIN dates_nodes_reference USING (local_date)
)

SELECT
src_node,
dst_node,
local_date,
weight,
src_rank

FROM
(
    SELECT
    *,
    RANK() OVER (PARTITION BY local_date, src_node ORDER BY weight DESC) AS src_rank
    FROM
    smoothed_weights)
    WHERE src_rank <= 100 AND weight > 0