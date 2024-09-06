# PROCESSES-SMC-POSTGRES-DYNAMIC

## Introduction
Creates the tables `SGPlaceRaw` and `SGBrandRaw` with the latest POI data, in the `smc_postgres` dataset.

## Contents
- [PROCESSES-SMC-POSTGRES-DYNAMIC](#processes-smc-postgres-dynamic)
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
- This DAG needs both `smc_sg_places.places_dynamic` and `smc_sg_places.brands_dynamic` to be up to date with the latest POI data.

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
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-postgres-dynamic)