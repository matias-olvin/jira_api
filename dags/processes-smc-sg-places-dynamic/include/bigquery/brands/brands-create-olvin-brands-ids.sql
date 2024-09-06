CREATE OR REPLACE TABLE
  `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['olvin_brand_ids_table'] }}`
COPY
  `{{ var.value.env_project }}.{{ params['sg_places_dataset'] }}.{{ params['olvin_brand_ids_table'] }}`
;

MERGE INTO 
  `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['olvin_brand_ids_table'] }}` olvin_brands
USING 
  `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['brands_dynamic_table'] }}` sg_brands
ON olvin_brands.fk_sgbrand = sg_brands.pid
WHEN NOT MATCHED THEN
  INSERT (
    fk_sgbrand,
    olvin_brand_id
  )
  VALUES (
    sg_brands.pid,
    generate_uuid()
  )
