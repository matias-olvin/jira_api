with both_a_b_b_a as
(
    select *
    from `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['observed_connections_edges_table'] }}`

    union all

    select dst_node as src_node, weight,  src_node as dst_node, relation
    from `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['observed_connections_edges_table'] }}`
)

select
    count(*) total , total_1 as node_edges
from
(
    SELECT src_node, count(*) total_1
    FROM both_a_b_b_a 
    group by src_node

)
    group by total_1
    order by node_edges