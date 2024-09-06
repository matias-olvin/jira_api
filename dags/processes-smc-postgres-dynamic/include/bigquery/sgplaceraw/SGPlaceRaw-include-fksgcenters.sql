CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}`
--  `storage-prod-olvin-com.smc_sg_places.SGPlaceRaw`
AS

SELECT a.* EXCEPT(fk_sgcenters), b.fk_sgcenters
FROM
  `{{ var.value.env_project }}.{{ params['smc_postgres_dataset'] }}.{{ params['sgplaceraw_table'] }}` a
--  `storage-prod-olvin-com.smc_sg_places.SGPlaceRaw` a
LEFT JOIN(
  SELECT pid as fk_sgcenters
  FROM
    `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['malls_base_table'] }}`
--    `storage-prod-olvin-com.smc_sg_places.malls_base`
) b
ON fk_parents = b.fk_sgcenters
