# PROCESSES-SMC-POST-CLUSTER-BACKFILL

## Introduction
This DAG serves to backfill the dates between `smc_start_date` and `smc_end_date` for all scaling tasks that run during `processes-smc`: it is
triggered once per date in `processes-smc-trigger`. This is needed because the scaling tasks in `processes-smc` only go up to `smc_start_date`.

## Contents
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [Changelog](#changelog)
- [\[1.0.0\] - 2024-03-26](#100---2024-03-26)
- [References](#references)

## Prerequisites
- This DAG is set to run for every day between `smc_start_date` and `smc_end_date` (they are variables in airflow set in `processes-smc-trigger`)

## Usage
- This DAG should be triggered by the `processes-smc-trigger` DAG.

## Configuration
- This DAG uses a local `config.yaml` file.
- This DAG uses the `execution_date` as the backfill date: it is set by the trigger in processes-smc-trigger

## Troubleshooting
- Most times, an extra day needs to be run manually to account for the day spent running this pipeline. If so, go to `Trigger DAG w/ config` in the UI
of this DAG and set the `execution_date` to `YYYY-MM-DDT03:00:00` where the year, month and day are the values of the date that is being manually triggered.

## Changelog
### [1.0.0] - 2024-03-27
- :tada: DAG documented - [@matias-olvin](https://github.com/matias-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-post-cluster-backfill)
- [Dashboard Link](https://lookerstudio.google.com/reporting/7f5ea8cf-0559-473b-b1b6-d053b48a6bf4/page/p_g2a8up2cbd)