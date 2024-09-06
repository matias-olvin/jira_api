WITH
  estimated_edges AS (
  SELECT
    * except(weight),
    ROUND(weight, 2) as weight
  FROM
    --`storage-dev-olvin-com.sg_networks_staging.estimated_journeys`
    `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['estimated_journeys_table'] }}`
     ),
  -- all places that are source or destiny
  places_list as (
      select 
        distinct fk_sgplaces 
      from
        (select src_node as fk_sgplaces
        from estimated_edges
        union all
        select src_node as fk_sgplaces
        from estimated_edges)
  ),
  -- outgoing grouping by src node
  outgoing as (
    SELECT
    src_node as fk_sgplaces,
    REPLACE(REPLACE(TO_JSON_STRING(ARRAY_AGG(STRUCT(CAST(dst_node AS STRING) AS dst_node,
                weight))), ',"weight"', ''), '"dst_node":', '') AS outgoing,
    FROM
    (select * from estimated_edges order by src_node, weight desc)
    GROUP BY
    src_node),
  -- incoming grouping by dst node
  incoming as (
    SELECT
    dst_node as fk_sgplaces,
    REPLACE(REPLACE(TO_JSON_STRING(ARRAY_AGG(STRUCT(CAST(src_node AS STRING) AS src_node,
                weight))), ',"weight"', ''), '"dst_node":', '') AS incoming,
    FROM
    (select * from estimated_edges order by dst_node, weight desc)
    GROUP BY
    dst_node)
-- left joins
select *,
      IF ("{{params['mode']}}" = "historical",
          PARSE_DATE('%F', "{{ ds.format('%Y-%m-01') }}"),
          DATE("{{ ds.format('%Y-%m-01') }}")
          ) AS local_date,
from places_list
left join outgoing using (fk_sgplaces)
left join incoming using (fk_sgplaces)
