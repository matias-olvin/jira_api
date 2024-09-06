### DICTIONARY HOLIDAYS

For each fk_sgplace:
- Category: To be used
- Identifiers for different holiday

At the moment all is set to true...

### TRIGGERING
This DAG is triggered externally as a task in `scaling_models_creation_trigger` pipeline.

Dictionary is written to `smc_regressors` dataset, then transferred to `regressors` dataset as a task in `scaling_models_creation_trigger` pipeline.
