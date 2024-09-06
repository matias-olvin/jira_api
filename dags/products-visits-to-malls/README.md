# PRODUCTS-VISITS-TO-MALLS

## Introduction

This pipeline produces the following tables:
- visits_to_malls_daily
- visits_to_malls_monthly

The visits_to_malls_daily table (which is used to create visits_to_malls_monthly) contains the predicted visits_to_malls per day for a given mall e.g., Monterey Marketplace. The predictions are made using a random forest regressor run in bigquery. The table is aggregated by month to create visits_to_malls_monthly. Both tables are then validated for nulls and duplicates. The tables are also verified through the following steps:
1. Check month to month ratio increase is below a certain threshold (excluding November, December months and COVID-19 re-openings)
2. Check mall visits is below a certain threshold
3. Check malls are not too sparse

## Contents
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
  - [\[2024-02-01\]](#2024-02-01)
- [Changelog](#changelog)
  - [\[1.2.0\] - 2024-2-29](#120---2024-2-29)
  - [\[1.1.0\] - 2023-10-23](#110---2023-10-23)
  - [\[1.0.0\] - 2023-9-22](#100---2023-9-22)
- [References](#references)

## Prerequisites
- Access to the dev and prod training data stored [here](https://docs.google.com/spreadsheets/d/13J4n2Ah93CJUny7q5Geja231ZyJ8fqvcM85gHFBwsDM/edit#gid=831888214) and [here](https://docs.google.com/spreadsheets/d/1rbOFQxWiJdvBelTztX83BrbcNpr5vqiQeDn-EkMKJp8/edit#gid=831888214) respectively

## Usage
- This DAG is triggered by the [products-postgres](https://github.com/olvin-com/airflow-dags/tree/main/dags/products-postgres) DAG
  - it is triggered twice, once using the postgres dataset and once using the postgres_batch dataset
- The training data has to be updated in the worksheet above if there is new data to add. 
A notification will be sent via slack when `ingest_training_data_from_google_sheets.check_training_data_in_google_sheets` task has failed as a reminder to check
- A task called `mark_success_to_continue_with_model_training` will fail to halt the DAG. This part is to check if it is necessary for the model to be re-trained.
 Below are the instructions to follow based on the different scenarios:
  - If the training data has changed since the previous DAG run, the task can be marked success
  - If the training data has not changed, the following tasks can be skipped: `create_ml_model`, `evaluate_ml_model`, `run_ml_model_task`. Note that these 3 tasks 
  can also be skipped if this DAG has already been triggered in the postgres stage of the products-postgres pipeline

## Configuration
- This DAG uses the `global` config file
- `var.value.env_project` needs to be `storage-dev-olvin-com` for testing, or `storage-prod-olvin-com` for production
- The execution dates for this pipeline being triggered in postgres and postgres_batch are `ds` and `dsT00:00:01+00:00` respectively
- This DAG requires `dataset_postgres_template` conf to be pushed via `TriggerDagRunOperator` in the parent DAG (products-postgres)
  - values should be `postgres` for postgres stage and `postgres_batch` for postgres_batch stage 

## Troubleshooting
### [2024-02-01]
- There was a permissions issue when trying to access the data using the olvin default credentials. This is no longer an issue due to a change in
google sheets ingestion methodology

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [1.2.0] - 2024-2-29
- :unlock: Implemented new secure way of ingesting google sheet data - [@matias-olvin](https://github.com/olvin-com/airflow-dags/pull/246/files)
### [1.1.0] - 2023-10-23
- :fire: Added forecasts further into the future rather than end of month in SQL code. Also combined the SQL code for verifications and made tables for metrics - [@matias-olvin](https://github.com/olvin-com/airflow-dags/pull/68/files)
### [1.0.0] - 2023-9-22
- :bulb: DAG code created - [@matias-olvin](https://github.com/olvin-com/airflow-dags/pull/36/files)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/products-visits-to-malls)
- [Confluence Documentation](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2376892451/products-visits-to-malls)
- [Dashboard](https://lookerstudio.google.com/reporting/c467b483-af62-4069-b520-764b95caaac9/page/EQHhD)