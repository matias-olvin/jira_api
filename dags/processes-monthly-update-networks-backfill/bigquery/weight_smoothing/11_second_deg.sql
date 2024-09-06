with observed_connections_edges_table as (
    SELECT
        *
    FROM
        `{{ params['project'] }}.{{ params['networks_dataset'] }}.{{ params['mixed_conn_table'] }}`
    left join ( select fk_sgplaces AS src_node, ST_GEOGPOINT(longitude, latitude) AS point_src
                from 
                    `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}` )
        USING(src_node)
    left join ( select fk_sgplaces AS dst_node, ST_GEOGPOINT(longitude, latitude) AS point_dst
                from 
                    `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}` )
        USING(dst_node)
    -- where
    --src_node = 'zzw-226@627-wc7-qmk'
  WHERE ST_DWITHIN(point_src, point_dst, 160e3)
  AND local_date = "{{ execution_date.strftime('%Y-%m-01') }}"
),

adding_percentiles as (
    select
    *,
    RANK() OVER ( PARTITION BY src_node, local_date ORDER BY weight ) / GREATEST(1, count(*)  OVER ( PARTITION BY src_node, local_date)) AS percentile_weight
    from observed_connections_edges_table
),
standarising_percentiles as (
    select *,
    (percentile_weight - avg(percentile_weight) over  ( PARTITION BY src_node, local_date)) /  IF(stddev(percentile_weight) over ( PARTITION BY src_node, local_date) = 0, 1, stddev(percentile_weight) over ( PARTITION BY src_node, local_date)) as   percentile_weight_std
    from
        adding_percentiles
),
removing_nulls as (
    SELECT * except(percentile_weight_std	),
    0.01 as percentile_weight_std	FROM standarising_percentiles
    where percentile_weight_std	IS NULL
    union all
    SELECT * FROM standarising_percentiles
    where percentile_weight_std	IS NOT NULL
),

transforming_to_exponential as (
    select
    *,
    exp(1.5 * percentile_weight_std)   as final_weight
    from removing_nulls
),
adding_percentages as (
select *,
final_weight / IF(sum(final_weight) over (partition by src_node, local_date) =0, 1, sum(final_weight) over (partition by src_node, local_date)) as percentage_weight
from transforming_to_exponential
),
transformed_table as (
    select src_node as src_node,
    dst_node as dst_node,
    local_date,
    percentage_weight as weight,
    point_src, point_dst
    from adding_percentages
),
raw_table AS (
  SELECT
    src_node,
    dst_node,
    local_date,
    SUM(weight) AS weight,
    ANY_VALUE(point_src) AS point_src ,
    ANY_VALUE(point_dst) AS point_dst,
    FROM
  (
      SELECT
      table_1.src_node,
      table_2.dst_node,
      ANY_VALUE(table_1.point_src) AS point_src,
      ANY_VALUE(table_2.point_dst) AS point_dst,
      table_1.local_date,
      SUM(table_1.weight*table_2.weight) AS weight,
  FROM
      transformed_table AS table_1
  JOIN
      transformed_table AS table_2
  ON table_1.dst_node = table_2.src_node AND table_1.local_date = table_2.local_date AND table_1.src_node <> table_2.dst_node
  AND ST_DWITHIN(table_1.point_src, table_2.point_dst, 160e3)
  -- WHERE table_1.src_node IN ('222-222@646-m35-hwk', "222-222@5pw-67r-c3q", "zzw-222@5pw-67r-fcq", "zzw-223@5pw-67r-bkz")
  GROUP BY
      table_1.src_node,
      table_2.dst_node,
      table_1.local_date
  UNION ALL
  SELECT
      src_node,
      dst_node,
      point_src,
      point_dst,
      local_date,
      weight
  FROM
      transformed_table
  -- WHERE src_node IN ('222-222@646-m35-hwk', "222-222@5pw-67r-c3q", "zzw-222@5pw-67r-fcq", "zzw-223@5pw-67r-bkz")
  )
  GROUP BY
      src_node,
      dst_node,
      local_date
)
SELECT
src_node, dst_node, local_date,
weight*(2*(1-POW(ST_DISTANCE(point_src, point_dst)/160e3, 2))*(1/POW(GREATEST(500/160e3, ST_DISTANCE(point_src, point_dst)/160e3), 1))) AS weight

FROM raw_table
JOIN
(SELECT pid AS src_node FROM
`{{ params['project'] }}.{{ params['places_postgres_dataset'] }}.{{ params['places_postgres_table'] }}`)
USING (src_node)