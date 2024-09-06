## DICTIONARY

The dictionary table assings for each POI which regresssors to take..

It will have the following columns:


- fk_sgplaces

- identifier_deaths(string, nullable)

- identifier_mobility(string, nullable)

- identifier_county_restrictions(string, nullable)

- identifier_city_restrictions(string, nullable)

- identifier_state_restrictions(string, nullable)


### IMPORTANT
Mobility: if state is DC we assign Maryland

### TRIGGERING
This DAG is triggered externally as a task in `scaling_models_creation_trigger` pipeline.

Dictionary is written to `smc_regressors` dataset, then transferred to `regressors` dataset as a task in `scaling_models_creation_trigger` pipeline.
