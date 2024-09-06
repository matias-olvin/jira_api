# COMMONS

The `commons` folder contains code or resources that can be shared across multiple DAGs. This should promote code reusability, reduce code duplication, and make it easier to maintain a DAGs.


## VARS
Contains variables used in code within the commons folder.

## UTILS

Functions that are used throughout DAGs.

  - `CALLBACKS`: Functions that are called by Airflow `Callbacks`.
  - `DAF_ARGS`: Functions for defining args passed to `DAG` constructor.
  - `FORMATTING`: Functions for consistent formatting.


## OPERATORS

Custom Operators to be used in DAGs.
  
  - `BIGQUERY`: Operators that run `BigQuery` Jobs.
  - `EXCEPTIONS`: Operators that raise `Exceptions`.
  - `TRIGGERS`: Operators for triggering workflows.
  - `VALIDATIONS`: Operators for running validations.


## PROCESSES

For sequences of Tasks that are frequently repeated across DAGs.

  - `MLOPS`: Processes for Machine Learning.
  - `TRIGGERS`: Processes for triggering DAGs.
  - `VALIDATIONS`: Processes for running validations on ground_truth data.