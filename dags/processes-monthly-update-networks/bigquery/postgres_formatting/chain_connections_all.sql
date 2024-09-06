WITH
  estimated_edges AS (
  SELECT
    src_node,
    fk_sgbrands,
    weight,
  FROM
    --`storage-dev-olvin-com.sg_networks_staging.estimated_connections`
    `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['estimated_connections_table'] }}`
  INNER JOIN (
      select
        pid as dst_node,
        fk_sgbrands
      from 
        --`storage-prod-olvin-com.sg_places.all-20210709`
    	`{{ params['project'] }}.{{ params['places_dataset'] }}.{{ params['places_table'] }}`

      where fk_sgbrands is not null
  ) USING  (dst_node)
  ),
  estimated_edges_to_chain AS (
      SELECT *
      FROM
        (SELECT
            src_node,
            fk_sgbrands,
            ROUND(sum(weight), 2) as weight
        FROM
            estimated_edges
        GROUP BY
            src_node, fk_sgbrands)
      ORDER BY 
        src_node, weight desc
  )
SELECT
  src_node as fk_sgplaces,
  REPLACE(REPLACE(TO_JSON_STRING(ARRAY_AGG(STRUCT(CAST(fk_sgbrands AS STRING) AS fk_sgbrands,
            weight))), ',"weight"', ''), '"fk_sgbrands":', '') AS connections,
  IF ("{{params['mode']}}" = "historical",
      PARSE_DATE('%F', "{{ execution_date.add(months=1).format('%Y-%m-01') }}"),
      DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}")
      ) AS local_date,
FROM
  estimated_edges_to_chain
GROUP BY
  src_node