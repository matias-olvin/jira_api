CREATE TEMP FUNCTION bin(x FLOAT64, y FLOAT64) AS
(
  
    case    
            when x/y <= 0.1 then '(0-0.1]'
            when x/y <= 0.2 then '(0.1-0.2]'
            when x/y <= 0.3 then '(0.2-0.3]'
            when x/y <= 0.4 then '(0.3-0.4]'
            when x/y <= 0.5 then '(0.4-0.5]'
            when x/y <= 0.6 then '(0.5-0.6]'
            when x/y <= 0.7 then '(0.6-0.7]'
            when x/y <= 0.8 then '(0.7-0.8]'
            when x/y <= 0.9 then '(0.8-0.9]'
            when x/y <= 1 then '(0.9-1]'
    end

);



with connections_edges_table as (
    SELECT 
        *
    FROM 
       -- `storage-prod-olvin-com.networks_staging.estimated_k_connections_transformed`
        -- `storage-prod-olvin-com.networks_staging.observed_connections_edges`
         `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ connections_edges_table }}`

),

-- Distance
nodes_with_distance as (
select
    connections_edges_table.*,
    ST_DISTANCE(f_r_1.point_1, f_r_2.point_2)
        as metric_distance

from connections_edges_table
left join ( select fk_sgplaces, ST_GEOGPOINT(longitude, latitude) as point_1 from
        --`storage-prod-olvin-com.networks_staging.node_features_raw`
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}`
        ) f_r_1
on connections_edges_table.src_node   = f_r_1.fk_sgplaces
left join ( select fk_sgplaces, ST_GEOGPOINT(longitude, latitude)  as point_2 from
        --`storage-prod-olvin-com.networks_staging.node_features_raw`
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}`
        ) f_r_2
on connections_edges_table.dst_node   = f_r_2.fk_sgplaces
),

adding_bin_distance as 
(
select *,
    bin(row_number() over (order by metric_distance) , total_count )  as bin_distance_weight

from nodes_with_distance
CROSS JOIN
(SELECT COUNT(metric_distance) AS total_count FROM nodes_with_distance)
),

medians_per_bin_distance as (
    select distinct 
        PERCENTILE_CONT(metric_distance, 0.5) AS metric_distance,
        PERCENTILE_CONT(weight, 0.5) AS weight,
        count(*)  AS total_in_bin,
        DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}") as local_date,
        'distance' as metric
    from adding_bin_distance
    GROUP BY bin_distance_weight
),


-- Life Stages 1
nodes_with_life_stages_1 as (
select
    connections_edges_table.*,
    abs(f_r_1.point_1 -  f_r_2.point_2)
        as metric_distance

from connections_edges_table
left join ( select fk_sgplaces, fk_life_stages_1 as point_1 from 
        --`storage-prod-olvin-com.networks_staging.node_features_raw`
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}`
        ) f_r_1
on connections_edges_table.src_node   = f_r_1.fk_sgplaces
left join ( select fk_sgplaces, fk_life_stages_1  as point_2 from 
        --`storage-prod-olvin-com.networks_staging.node_features_raw`
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}`
        ) f_r_2
on connections_edges_table.dst_node   = f_r_2.fk_sgplaces
),

adding_bin_life_stages_1 as 
(
select *,
    bin(row_number() over (order by metric_distance) , total_count )  as bin_distance_weight

from nodes_with_life_stages_1
CROSS JOIN
(SELECT COUNT(metric_distance) AS total_count FROM nodes_with_life_stages_1)
),

medians_per_bin_life_stages_1 as (
    select distinct 
        PERCENTILE_CONT(metric_distance, 0.5) AS metric_distance,
        PERCENTILE_CONT(weight, 0.5) AS weight,
        count(*) AS total_in_bin,
        DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}") as local_date,
        'fk_life_stages_1' as metric

    from adding_bin_life_stages_1
    GROUP BY bin_distance_weight
),

-- fk_incomes_5
nodes_with_incomes_5 as (
select
    connections_edges_table.*,
    abs(f_r_1.point_1 -  f_r_2.point_2)
        as metric_distance

from connections_edges_table
left join ( select fk_sgplaces, fk_incomes_5 as point_1 from 
        --`storage-prod-olvin-com.networks_staging.node_features_raw`
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}`
        ) f_r_1
on connections_edges_table.src_node   = f_r_1.fk_sgplaces
left join ( select fk_sgplaces, fk_incomes_5  as point_2 from 
        --`storage-prod-olvin-com.networks_staging.node_features_raw`
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}`
        ) f_r_2
on connections_edges_table.dst_node   = f_r_2.fk_sgplaces
),

adding_bin_incomes_5 as 
(
select *,
    bin(row_number() over (order by metric_distance) , total_count )  as bin_distance_weight

from nodes_with_incomes_5
CROSS JOIN
(SELECT COUNT(metric_distance) AS total_count FROM nodes_with_incomes_5)
),

medians_per_bin_incomes_5 as (
    select distinct 
        PERCENTILE_CONT(metric_distance, 0.5) AS metric_distance,
        PERCENTILE_CONT(weight, 0.5) AS weight,
        count(*) AS total_in_bin,
        DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}") as local_date,
        'fk_incomes_5' as metric
    from adding_bin_incomes_5
    GROUP BY bin_distance_weight
),


-- visits_morning
nodes_with_visits_morning as (
select
    connections_edges_table.*,
    abs(f_r_1.point_1 -  f_r_2.point_2)
        as metric_distance

from connections_edges_table
left join ( select fk_sgplaces, visits_morning as point_1 from 
        --`storage-prod-olvin-com.networks_staging.node_features_raw`
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}`
        ) f_r_1
on connections_edges_table.src_node   = f_r_1.fk_sgplaces
left join ( select fk_sgplaces, visits_morning  as point_2 from 
        --`storage-prod-olvin-com.networks_staging.node_features_raw`
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}`
        ) f_r_2
on connections_edges_table.dst_node   = f_r_2.fk_sgplaces
),

adding_bin_visits_morning as 
(
select *,
    bin(row_number() over (order by metric_distance) , total_count )  as bin_distance_weight

from nodes_with_visits_morning
CROSS JOIN
(SELECT COUNT(metric_distance) AS total_count FROM nodes_with_visits_morning)
),

medians_per_bin_visits_morning as (
    select distinct 
        PERCENTILE_CONT(metric_distance, 0.5) AS metric_distance,
        PERCENTILE_CONT(weight, 0.5) AS weight,
        count(*) AS total_in_bin,
        DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}") as local_date,
        'visits_morning' as metric
    from adding_bin_visits_morning
    GROUP BY bin_distance_weight
),

-- visits_weekend
nodes_with_visits_weekend as (
select
    connections_edges_table.*,
    abs(f_r_1.point_1 -  f_r_2.point_2)
        as metric_distance

from connections_edges_table
left join ( select fk_sgplaces, visits_weekend as point_1 from 
        --`storage-prod-olvin-com.networks_staging.node_features_raw`
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}`
        ) f_r_1
on connections_edges_table.src_node   = f_r_1.fk_sgplaces
left join ( select fk_sgplaces, visits_weekend  as point_2 from 
        --`storage-prod-olvin-com.networks_staging.node_features_raw`
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}`
        ) f_r_2
on connections_edges_table.dst_node   = f_r_2.fk_sgplaces
),

adding_bin_visits_weekend as 
(
select *,
    bin(row_number() over (order by metric_distance) , total_count )  as bin_distance_weight

from nodes_with_visits_weekend
CROSS JOIN
(SELECT COUNT(metric_distance) AS total_count FROM nodes_with_visits_weekend)
),

medians_per_bin_visits_weekend as (
    select distinct 
        PERCENTILE_CONT(metric_distance, 0.5) AS metric_distance,
        PERCENTILE_CONT(weight, 0.5) AS weight,
        count(*) AS total_in_bin,
        DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}") as local_date,
        'visits_weekend' as metric
    from adding_bin_visits_weekend
    GROUP BY bin_distance_weight
),

-- visits_total
nodes_with_visits_total as (
select
    connections_edges_table.*,
    f_r_2.point_2
        as metric_distance

from connections_edges_table
left join ( select fk_sgplaces, visits_total as point_1 from 
        --`storage-prod-olvin-com.networks_staging.node_features_raw`
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}`
        ) f_r_1
on connections_edges_table.src_node   = f_r_1.fk_sgplaces
left join ( select fk_sgplaces, visits_total  as point_2 from 
        --`storage-prod-olvin-com.networks_staging.node_features_raw`
        `{{ params['project'] }}.{{ params['networks_staging_dataset'] }}.{{ params['poi_representation_table'] }}`
        ) f_r_2
on connections_edges_table.dst_node   = f_r_2.fk_sgplaces
),

adding_bin_visits_total as 
(
select *,
    bin(row_number() over (order by metric_distance) , total_count )  as bin_distance_weight

from nodes_with_visits_total
CROSS JOIN
(SELECT COUNT(metric_distance) AS total_count FROM nodes_with_visits_total)
),

medians_per_bin_visits_total as (
    select distinct 
        PERCENTILE_CONT(metric_distance, 0.5) AS metric_distance,
        PERCENTILE_CONT(weight, 0.5) AS weight,
        count(*) AS total_in_bin,
        DATE("{{ execution_date.add(months=1).format('%Y-%m-01') }}") as local_date,
        'visits_total' as metric
    from adding_bin_visits_total
    GROUP BY bin_distance_weight
)




select *
from medians_per_bin_distance

union all 
select *
from medians_per_bin_life_stages_1

union all 
select *
from medians_per_bin_incomes_5

union all 
select *
from medians_per_bin_visits_morning

union all 
select *
from medians_per_bin_visits_weekend

union all 
select *
from medians_per_bin_visits_total
