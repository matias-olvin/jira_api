# PROCESSES-SMC-REGRESSORS-MIGRATIOn

## Introduction
Copies tables from `smc_regressors` dataset to `regressors` dataset and tables from `smc_events` dataset to `events` dataset.

## Contents
- [PROCESSES-SMC-REGRESSORS-MIGRATIOn](#processes-smc-regressors-migration)
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
- This should only be run after all other SMC processes have finished.

## Usage
- This DAG should be triggered by the `process-smc-trigger` DAG.

## Configuration
- This DAG uses the global `common/config.yaml` file.

## Troubleshooting
- No known issues.

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [1.0.0] - 2024-02-13
- :tada: DAG documented - [@jake-olvin](https://github.com/jake-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-regressors-migration)