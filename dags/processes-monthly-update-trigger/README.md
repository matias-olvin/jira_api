# PROCESSES-MONTHLY-UPDATE-TRIGGER

## Introduction

This DAG is the parent DAG to all processes involved in the monthly update. It is manually triggered every month, with an execution_date that is determined via the latest execution_date in `storage-prod-olvin-com.test_compilation.monthly_update` `WHERE completed = TRUE` + 1 Month: a new row is created with `completed` column set to `FALSE`.

This DAG has triggers for the following child DAGs:

- `processes-monthly-update-demographics`
- `processes-monthly-update-agg-stats`
- `processes-monthly-update-cameo-profiles`
- `processes-monthly-update-visitor-destination`
- `processes-monthly-update-visits-estimation-postgres`
- `processes-monthly-update-visits-estimation`
- `processes-monthly-update-void-market-preprocess`
- `products-postgres`

## Contents
  - [Prerequisites](#prerequisites)
  - [Usage](#usage)
  - [Configuration](#configuration)
  - [Troubleshooting](#troubleshooting)
    <!-- - [\[2024-01-01\]](#2024-01-01) -->
    - None so far
  - [Changelog](#changelog)
    - [\[1.0.2\] - 2024-4-11](#102---2024-4-11)
    - [\[1.0.1\] - 2024-1-23](#101---2024-1-23)
    - [\[1.0.0\] - 2023-9-25](#100---2023-9-25)
  - [References](#references)

## Prerequisites
- Previous monthly update should be completed before running again
- The code for the child DAGs and this DAG should be tested and merged into main before running

## Usage
- This DAG is run as its own process and is manually triggered
- There are 4 mark_success_to_update_{DAG_ID} tasks that have to be addressed:
  - mark_success_to_update_processes-monthly-update-visits-estimation will trigger processes-monthly-update-visits-estimation if marked success. If marked as failed, it will trigger processes-monthly-update-visits-estimation-postgres
  - mark_success_to_update-(demographics | visitor-destination | void-market-preprocess) can be marked as success when ready

## Configuration
- The DAG uses a local config.yaml file

## Troubleshooting
<!-- ### [2024-01-01] -->
- No issues so far

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [1.0.2] - 2024-4-11
- :+1: Changed order of DAG, removed deprecated dags from being triggered, refactored files - [@matias-olvin](https://github.com/matias-olvin)
### [1.0.1] - 2024-1-23
- :tada: DAG documented - [@matias-olvin](https://github.com/matias-olvin)
### [1.0.0] - 2023-9-25
- :mag_right: Restructured DAG - [@kartikeu](https://github.com/kartikeu)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-monthly-update-trigger)
- [Confluence Documentation](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2132017153/Monthly+Update+Trigger)
