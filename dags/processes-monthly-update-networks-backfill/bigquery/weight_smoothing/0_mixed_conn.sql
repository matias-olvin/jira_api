WITH observed_connections AS (
  SELECT * EXCEPT(weight), SQRT(weight) AS weight
   FROM 
    `{{ params['project'] }}.{{ params['networks_dataset'] }}.{{ params['observed_connections_edges_table'] }}`
   WHERE local_date = "{{ execution_date.strftime('%Y-%m-01') }}"
),
place_weights AS (
  SELECT
  src_node,
local_date,
AVG(weight) AS weight_total
 FROM observed_connections
GROUP BY src_node, local_date
)
SELECT *
-- EXCEPT(src_order)
FROM
(SELECT
src_node,
dst_node,
-- ROW_NUMBER() OVER (PARTITION BY src_node, local_date ORDER BY weight DESC) AS src_order,
weight,
local_date
FROM
(SELECT
src_node,
dst_node,
IFNULL(weight/NULLIF(weight_total, 0), 0) AS weight,
local_date
FROM
observed_connections
JOIN place_weights
USING(src_node, local_date)
UNION ALL
SELECT
src_node,
dst_node,
weight,
local_date
FROM 
  `{{ params['project'] }}.{{ params['networks_dataset'] }}.{{ params['predicted_mapping_id_table'] }}`
   WHERE local_date = "{{ execution_date.strftime('%Y-%m-01') }}"
))
  -- WHERE src_order <= 200