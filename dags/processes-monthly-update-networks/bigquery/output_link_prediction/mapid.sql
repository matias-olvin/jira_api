with original_table as  (
    select *
    from 
    --`storage-dev-olvin-com.sg_networks_staging.predicted_edges_table`
    `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['predicted_edges_table'] }}`
),

mapped_src_node as(
select cast(i.fk_sgplaces as string)  as src_node,
t.dst_node as dst_node1,
t.weight 
from original_table as t 
inner join 
`{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['neo4j_id_table'] }}`
--`storage-dev-olvin-com.sg_networks_staging.neo4j_id`
as i 
on cast (t.src_node as string)= cast(i.ID as string) 
),

mapped_dst_node as(
select src_node, 
cast(i.fk_sgplaces as string) as dst_node,
t.weight as weight 
from mapped_src_node as t 
inner join 
--`storage-dev-olvin-com.sg_networks_staging.neo4j_id`
`{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['neo4j_id_table'] }}`
as i 
on cast (t.dst_node1 as string) = cast(i.ID as string) 
),

-- match_src_node_brand as (
-- select t.src_node,
-- t.dst_node,
-- t.weight, 
-- f.brand_boolean as src_brand 
-- from mapped_dst_node as t 
-- inner join 
-- --`storage-dev-olvin-com.sg_networks_staging.nodes_features`
-- `{{ params['project'] }}.{{ params['networks_dataset'] }}.{{ params['node_features_table'] }}`
-- as f 
-- on t.src_node=f.fk_sgplaces),

-- match_dst_node_brand as (
-- select t.src_node,
-- t.dst_node,
-- t.src_brand,
-- t.weight,
-- f.brand_boolean as dst_brand 
-- from match_src_node_brand as t 
-- inner join 
-- --`storage-dev-olvin-com.sg_networks_staging.nodes_features`
-- `{{ params['project'] }}.{{ params['networks_dataset'] }}.{{ params['node_features_table'] }}`
-- as f 
-- on t.dst_node=f.fk_sgplaces),


drop_duplicates  AS
(
SELECT * EXCEPT(row_number)
FROM (
  SELECT
      *,
      ROW_NUMBER()
          OVER (PARTITION BY src_node,dst_node)
          as row_number
  FROM mapped_dst_node
)
WHERE row_number = 1
)
SELECT
  *
FROM drop_duplicates


