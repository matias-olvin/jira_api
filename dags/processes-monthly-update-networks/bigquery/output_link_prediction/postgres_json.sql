with estimated_edges AS (
  SELECT
    * except (weight),
    ROUND(weight, 2) as weight
  FROM
    --`storage-prod-olvin-com.sg_networks_staging.estimated_connections`
     `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['estimated_connections_table'] }}`
)


SELECT
  src_node as fk_sgplaces,
  REPLACE(REPLACE(TO_JSON_STRING(ARRAY_AGG(STRUCT(CAST(dst_node AS STRING) AS dst_node,
            weight))), ',"weight"', ''), '"dst_node":', '') AS connections,
      -- IF ("'mode'" = "historical",
      -- PARSE_DATE('%F', "{{ execution_date.add(months=1).format('%Y-%m-01') }}"),
      -- DATE_TRUNC(DATE_SUB( CAST("{{ execution_date.add(months=1).format('%Y-%m-01') }}" AS DATE), INTERVAL 0 MONTH), MONTH)
      -- ) AS local_date,
FROM
  estimated_edges

GROUP BY
  src_node