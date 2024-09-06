
with joining_places_we_want as 
(
	SELECT
		node_features_all.* except(olvin_category),
		ifnull(dummy, False) as dummy
	FROM
		--`storage-prod-olvin-com.networks_staging.node_features_all` node_features_all
		`{{ params['project'] }}.{{ params['poi_representation_dataset'] }}.{{ params['node_features_all_table'] }}` node_features_all

	inner join 
		(select distinct pid as fk_sgplaces from
				--`storage-prod-olvin-com.postgres.SGPlaceRaw`
				`{{ params['project'] }}.{{ params['places_postgres_dataset'] }}.{{ params['places_postgres_table'] }}`
		) using (fk_sgplaces)
	left join 
		(select distinct  fk_sgplaces, True as dummy from 
			`{{ params['project'] }}.networks_staging.sample_poi_list`
		) using (fk_sgplaces)
),

filtering_if_sample as 
(
select * except (dummy)
from joining_places_we_want
where dummy = True or {{ is_sample }} = False
)

select fk_sgplaces as node_id,*
from filtering_if_sample


