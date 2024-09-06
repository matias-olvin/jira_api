# PROCESSES-SMC-POI-VISITS-BACKFILL

## Introduction
Backfills `poi_visits` for all dates up to `smc_start_date` (-9 days latency) to include the latest poi data, storing tables in `smc_poi_visits`.

## Contents
- [PROCESSES-SMC-POI-VISITS-BACKFILL](#processes-smc-poi-visits-backfill)
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
- This DAG assumes starting date for backfill is `2018-01-01`.

## Usage
- This DAG should be triggered by the `process-smc-trigger` DAG.

## Configuration
- This DAG uses a local `cconfig.yaml` file.
- This DAG relies on an Airflow Variable: `smc_start_date`.

## Troubleshooting
- No known issues.

## Changelog
### [1.0.0] - 2024-02-13
- :tada: DAG documented - [@jake-olvin](https://github.com/jake-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-poi-visits-backfill)