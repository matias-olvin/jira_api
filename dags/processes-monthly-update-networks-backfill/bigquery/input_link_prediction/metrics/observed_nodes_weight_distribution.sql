with both_a_b_b_a as
(
    select *
    from `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['observed_connections_edges_table'] }}`

    union all

    select dst_node as src_node, weight,  src_node as dst_node, relation
    from `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['observed_connections_edges_table'] }}`
),

table_pivoted as (select DISTINCT
    PERCENTILE_CONT(total_weight, 0.1) OVER() AS percentile10,
    PERCENTILE_CONT(total_weight, 0.2) OVER() AS percentile20,
    PERCENTILE_CONT(total_weight, 0.3) OVER() AS percentile30,
    PERCENTILE_CONT(total_weight, 0.4) OVER() AS percentile40,
    PERCENTILE_CONT(total_weight, 0.5) OVER() AS percentile50,
    PERCENTILE_CONT(total_weight, 0.6) OVER() AS percentile60,
    PERCENTILE_CONT(total_weight, 0.7) OVER() AS percentile70,
    PERCENTILE_CONT(total_weight, 0.8) OVER() AS percentile80,
    PERCENTILE_CONT(total_weight, 0.9) OVER() AS percentile90,

from

    (SELECT src_node, sum(weight) total_weight
    FROM both_a_b_b_a 
    group by src_node)
)

SELECT *

FROM table_pivoted

UNPIVOT 
(percentile_value FOR percentile 
IN (percentile10,percentile20,percentile30,percentile40,percentile50, percentile60, percentile70,percentile80,percentile90))
