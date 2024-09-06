# PROCESSES-SMC-PLACES-MIGRATION

## Introduction
Copies tables from `smc_sg_places` dataset to `sg_places` dataset and `smc_postgres` to `postgres` dataset.

## Contents
- [PROCESSES-SMC-PLACES-MIGRATION](#processes-smc-places-migration)
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
### [1.0.0] - 2024-02-13
- :tada: DAG documented - [@jake-olvin](https://github.com/jake-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-places-migration)