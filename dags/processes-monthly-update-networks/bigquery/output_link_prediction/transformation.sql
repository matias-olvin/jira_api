with original_table as (
    SELECT src_node, 
    dst_node, 
    weight as weight
    --FROM `storage-prod-olvin-com.sg_networks_staging.predicted_chaintochain`
    FROM 
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['predicted_mapping_id_table'] }}`
),


less_than_x_miles as (
    select * except (distance) 
    from
        (
            select
                original_table.*,
                ST_DISTANCE(f_r_1.point_1, f_r_2.point_2) as distance

            from original_table
            left join ( select fk_sgplaces, ST_GEOGPOINT(longitude, latitude) as point_1 from
                    -- `storage-prod-olvin-com.networks_staging.node_features_raw`
                    `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['node_features_raw_table'] }}`
                    ) f_r_1
            on original_table.src_node   = f_r_1.fk_sgplaces
            left join ( select fk_sgplaces, ST_GEOGPOINT(longitude, latitude) as point_2 from
                    -- `storage-prod-olvin-com.networks_staging.node_features_raw`
                    `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['node_features_raw_table'] }}`
                    ) f_r_2
            on original_table.dst_node   = f_r_2.fk_sgplaces
        )
    where distance < (1600 * cast("{{params['max_miles']}}" as float64))
),


limiting_to_k_connections_per_node as (
    select * except(weight_rank)
    from 
    (
        SELECT
            *,
            row_number() over (partition by src_node order by weight desc) as weight_rank
        FROM less_than_x_miles
    )
    where weight_rank <= cast("{{params['topk']}}" as int64)
),


adding_percentiles as (
    select
    *,
    RANK() OVER ( PARTITION BY src_node ORDER BY weight ) / count(*)  OVER ( PARTITION BY src_node) AS percentile_weight
    from limiting_to_k_connections_per_node
),
adding_noise_to_percentiles as (
    select
    * except(percentile_weight),
    percentile_weight + (0.05 * (rand() - 0.5) ) AS percentile_weight,
    from adding_percentiles
),
standarising_percentiles as (
    select *,
    (percentile_weight - avg(percentile_weight) over  ( PARTITION BY src_node)) /  (stddev(percentile_weight) over ( PARTITION BY src_node)) as   percentile_weight_std
    from
        adding_noise_to_percentiles
),
removing_nulls as (
    SELECT * except(percentile_weight_std	),
    0.01 as percentile_weight_std	FROM standarising_percentiles 
    where percentile_weight_std	IS NULL
    union all
    SELECT * FROM standarising_percentiles 
    where percentile_weight_std	IS NOT NULL
),

transforming_to_exponential as (
    select
    *,
    exp(1.5 * percentile_weight_std)   as final_weight
    from removing_nulls
),
adding_percentages as (
select *,
100 * final_weight / sum(final_weight) over (partition by src_node) as percentage_weight
from transforming_to_exponential
),
transformed_table as (
    select src_node as src_node,
    dst_node as dst_node,
    percentage_weight as weight
    from adding_percentages
)
select *
from transformed_table 


