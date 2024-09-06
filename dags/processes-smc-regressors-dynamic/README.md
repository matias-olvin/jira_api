# PROCESSES-SMC-REGRESSORS-DYNAMIC

## Introduction
Triggers each of the regressor pipelines that need to be updated with the latest `fk_sgplaces` from `places_dynamic` table.
- DAG IDs are defined in an array in the `config.yaml` file assigned to `regressor_dictionary_pipelines` and referenced in a list (`DICTIONARIES`) in `trigger_regressor_pipelines.py` file.

## Contents
- [PROCESSES-SMC-REGRESSORS-DYNAMIC](#processes-smc-regressors-dynamic)
  - [Introduction](#introduction)
  - [Contents](#contents)
  - [Prerequisites](#prerequisites)
  - [Usage](#usage)
  - [Configuration](#configuration)
  - [Troubleshooting](#troubleshooting)
  - [Changelog](#changelog)
    - [\[1.0.0\] - 2024-02-13](#100---2024-02-13)
  - [References](#references)

## Prerequisites
- This DAG needs `smc_sg_places.places_dynamic` to be up to date with the latest POI data.

## Usage
- This DAG should be triggered by the `process-smc-trigger` DAG.

## Configuration
- This DAG uses the global `common/config.yaml` file.

## Troubleshooting
- No known issues.

## Changelog
### [1.0.0] - 2024-02-13
- :tada: DAG documented - [@jake-olvin](https://github.com/jake-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-regressors-dynamic)