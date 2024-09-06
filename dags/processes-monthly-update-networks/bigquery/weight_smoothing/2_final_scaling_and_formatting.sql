WITH adding_percentiles as (
    select
    *,
    RANK() OVER ( PARTITION BY src_node ORDER BY weight ) / GREATEST(count(*)  OVER ( PARTITION BY src_node, local_date), 1) AS percentile_weight
    from 
      `{{ params['project'] }}.{{ params['networks_dataset'] }}.{{ params['smooth_conn_table'] }}`
),
standarising_percentiles as (
    select *,
    (percentile_weight - avg(percentile_weight) over  ( PARTITION BY src_node, local_date)) /  GREATEST(stddev(percentile_weight) over ( PARTITION BY src_node, local_date), 0.00001) as   percentile_weight_std
    from
        adding_percentiles
),
removing_nulls as (
    SELECT * except(percentile_weight_std),
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
100 * final_weight / GREATEST(sum(final_weight) over (partition by src_node, local_date), 0.00001) as percentage_weight
from transforming_to_exponential
),
transformed_table as (
    select src_node as src_node,
    dst_node as dst_node,
    local_date,
    percentage_weight as weight
    from adding_percentages
),
estimated_edges AS (
  SELECT
    * except (weight),
    ROUND(weight, 2) as weight,
    COUNT(*) OVER (PARTITION BY local_date, src_node) AS total_node
  FROM
    --`storage-prod-olvin-com.sg_networks_staging.estimated_connections`
     transformed_table
)

SELECT * FROM
(SELECT
  src_node as fk_sgplaces,
  local_date,
  REPLACE(REPLACE(TO_JSON_STRING(ARRAY_AGG(STRUCT(CAST(dst_node AS STRING) AS dst_node,
            weight))), ',"weight"', ''), '"dst_node":', '') AS connections,
      -- IF ("'mode'" = "historical",
      -- PARSE_DATE('%F', "{{ execution_date.add(months=1).format('%Y-%m-01') }}"),
      -- DATE_TRUNC(DATE_SUB( CAST("{{ execution_date.add(months=1).format('%Y-%m-01') }}" AS DATE), INTERVAL 0 MONTH), MONTH)
      -- ) AS local_date,
FROM
  estimated_edges
 WHERE total_node >= 20

GROUP BY
  src_node, local_date)
JOIN
(SELECT fk_sgplaces FROM 
  `{{ params['project'] }}.{{ params['postgres_dataset'] }}.{{ params['activity_table'] }}`)
USING(fk_sgplaces)