WITH
  all_combinations AS (
  SELECT
    *
  FROM
    --`storage-dev-olvin-com.sg_networks_staging.all_combinations`
    `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['all_combinations_table'] }}`
    WHERE
      local_date = "{{ ds.format('%Y-%m-01') }}"
  ),

connection_edges as (
SELECT
  src_node,
  SUM(weight) AS weight,
  dst_node, 
  'RELATION' AS relation
FROM
  all_combinations
GROUP BY
  src_node,
  dst_node )


SELECT src_node, weight, dst_node, relation, DATE("{{ ds.format('%Y-%m-01') }}") AS local_date
FROM connection_edges