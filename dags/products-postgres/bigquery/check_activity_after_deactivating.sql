SELECT COUNT(*) - COUNTIF(in_blacklist)
FROM
  `{{ params['project'] }}.{{ params['postgres_dataset'] }}.{{  params['sgplaceactivity_table'] }}` postgres
-- `storage-prod-olvin-com.postgres.SGPlaceActivity` postgres
INNER JOIN
  `{{ params['project'] }}.{{ params['postgres_batch_dataset'] }}.{{  params['sgplaceactivity_table'] }}` postgres_batch
-- `storage-prod-olvin-com.postgres_batch.SGPlaceActivity` postgres_batch
USING(fk_sgplaces)
LEFT JOIN(
  SELECT pid AS fk_sgplaces, TRUE AS in_blacklist
  FROM
  `{{ params['project'] }}.{{ params['sg_places_dataset'] }}.{{ params['wrong_coords_blacklist_table'] }}`
-- `storage-prod-olvin-com.sg_places.wrong_coords_blacklist`
)
USING(fk_sgplaces)
WHERE postgres.activity <> postgres_batch.activity