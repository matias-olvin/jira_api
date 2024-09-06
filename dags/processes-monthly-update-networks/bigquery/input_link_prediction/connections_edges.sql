WITH
  all_combinations AS (
  SELECT
    *
  FROM
    --`storage-dev-olvin-com.sg_networks_staging.all_combinations`
    `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['all_combinations_table'] }}`
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
  dst_node ),
all_nodes_table AS (
  SELECT DISTINCT
  fk_sgplaces
  FROM `{{ params['project'] }}.{{ params['places_postgres_dataset'] }}.{{ params['places_postgres_table'] }}`
  JOIN `{{ params['project'] }}.{{ params['networks_dataset'] }}.{{ params['node_features_table'] }}`
  ON node_id = fk_sgplaces
)


SELECT src_node, weight, dst_node, relation
FROM connection_edges
WHERE src_node IN (SELECT fk_sgplaces FROM all_nodes_table)
AND dst_node IN (SELECT fk_sgplaces FROM all_nodes_table)