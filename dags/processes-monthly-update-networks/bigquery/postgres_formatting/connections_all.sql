WITH
  estimated_edges AS (
  SELECT
    * except (weight),
    ROUND(weight, 2) as weight
  FROM
    --`storage-dev-olvin-com.sg_networks_staging.estimated_connections`
    `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['estimated_connections_table'] }}`
  ORDER BY
    src_node,
    weight DESC )
SELECT
  src_node as fk_sgplaces,
  REPLACE(REPLACE(TO_JSON_STRING(ARRAY_AGG(STRUCT(CAST(dst_node AS STRING) AS dst_node,
            weight))), ',"weight"', ''), '"dst_node":', '') AS connections,
  IF ("{{params['mode']}}" = "historical",
      PARSE_DATE('%F', "{{ execution_date.add(months=1).format('%Y-%m-01') }}"),
      DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}")
      ) AS local_date,
FROM
  estimated_edges
GROUP BY
  src_node