DECLARE list_L ARRAY<STRING>;

SET list_L = {{ '["' + params['smc_demand_places'] | join('", "') + '"]' }};

CREATE OR REPLACE TABLE 
`{{ var.value.env_project }}.{{ params['smc_demand_dataset'] }}.{{ params['places_demand_table'] }}`(
    fk_sgplaces STRING OPTIONS(description="Place ID"),
    current_filter BOOL OPTIONS(description="Current boolean value for processing Place ID")
) AS 
SELECT pid as fk_sgplaces, 
TRUE as current_filter,
"initialize" as change_reason
FROM UNNEST(list_L) ;