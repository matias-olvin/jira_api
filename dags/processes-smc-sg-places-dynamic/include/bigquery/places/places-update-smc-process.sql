DECLARE list_L ARRAY<STRING>;

CREATE TABLE IF NOT EXISTS 
`{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['sg_places_filter_table'] }}`(
    fk_sgplaces STRING OPTIONS(description="Place ID"),
    previous_filter BOOL OPTIONS(description="Previous boolean value for processing Place ID"),
    current_filter BOOL OPTIONS(description="Current boolean value for processing Place ID"),
    change_reason STRING OPTIONS(description="Reason for change of boolean value for Place ID(SMC/Manual change/naics_code etc)"),
    update_date DATE OPTIONS(description="Date of change for Place ID")
) AS 
SELECT pid as fk_sgplaces, 
FALSE as previous_filter, 
FALSE as current_filter,
"initialize" as change_reason,
DATE("1990-01-01") as update_date
FROM `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}` ;

SET list_L = {{ '["' + params['naics_codes_smc'] | join('", "') + '"]' }};

MERGE `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['sg_places_filter_table'] }}` filter_table
USING `{{ var.value.env_project }}.{{ params['smc_sg_places_dataset'] }}.{{ params['places_dynamic_table'] }}` places_table
ON filter_table.fk_sgplaces = places_table.pid
WHEN MATCHED AND (places_table.fk_sgbrands is not null OR CAST(places_table.naics_code as string) IN UNNEST(list_L)) THEN
  UPDATE SET filter_table.previous_filter = filter_table.current_filter,
  filter_table.current_filter = TRUE,
  filter_table.change_reason = "SMC",
  filter_table.update_date = DATE("{{ ds }}")
WHEN NOT MATCHED AND (places_table.fk_sgbrands is not null OR CAST(places_table.naics_code as string) IN UNNEST(list_L)) THEN
  INSERT(fk_sgplaces, previous_filter, current_filter, change_reason, update_date)
  VALUES(places_table.pid, FALSE, TRUE , "SMC", DATE('{{ ds }}'))
WHEN NOT MATCHED AND (places_table.fk_sgbrands is null OR CAST(places_table.naics_code as string) not IN UNNEST(list_L)) THEN
  INSERT(fk_sgplaces, previous_filter, current_filter, change_reason, update_date)
  VALUES(places_table.pid, FALSE, FALSE , "SMC", DATE('{{ ds }}'));