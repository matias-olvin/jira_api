# PROCESSES-SMC-SNS-METRICS

## Introduction
Collects GTVM volume metrics from SNS in `accessible_by_olvin` and inserts them into `smc_metrics.gtvm_visits_accuracy`.

## Contents
- [PROCESSES-SMC-SNS-METRICS](#processes-smc-sns-metrics)
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
- This DAG needs to be run after the GTVM volume metrics pipeline has run in SNS.

## Usage
- This DAG should be triggered by the `process-smc-trigger` DAG.

## Configuration
- This DAG uses a local  `cconfig.yaml` file.

## Troubleshooting
- No known issues.

## Changelog
### [1.0.0] - 2024-02-13
- :tada: DAG documented - [@jake-olvin](https://github.com/jake-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-sns-metrics)