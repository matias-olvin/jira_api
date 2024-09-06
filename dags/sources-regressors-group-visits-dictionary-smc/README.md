# README

## places
Create dictionary for all the places in the places table and their identifiers. Schema: pid | identifier_category | identifier_place

## zipcodes
Create dictionary for all the places in the places table and their relevant identifiers. Schema: pid | identifier_place

### TRIGGERING
This DAG is triggered externally as a task in `scaling_models_creation_trigger` pipeline.

Dictionary is written to `smc_regressors` dataset, then transferred to `regressors` dataset as a task in `scaling_models_creation_trigger` pipeline.
