with nodesfeatures as (SELECT 
    *
FROM  
-- Using only the historical features table for now
--`storage-dev-olvin-com.sg_networks_staging.node_features_historical`
`{{ params['project'] }}.{{ params['poi_representation_dataset'] }}.{{ params['poi_representation_table'] }}`)
select * 
from nodesfeatures
