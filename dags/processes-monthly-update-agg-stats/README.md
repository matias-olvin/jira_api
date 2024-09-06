# PROCESSES-MONTHLY-UPDATE-AGG-STATS

## Introduction
This pipeline is used to obtain an estimated home location of the POI visitors: this information can be used to obtain the profile of the visitors.
. This pipeline is run as part of the Monthly Update and is triggered by `processes-monthly-update-trigger`

## Contents
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
<!-- - [\[2024-01-01\]](#2024-01-01) -->
- [Changelog](#changelog)
- [\[1.0.0\] - 2024-05-22](#100---2024-05-22)
- [References](#references)

## Prerequisites
- This pipeline requires the Airflow var `env_project` to be set to `storage-dev-olvin-com` in dev env, and `storage-prod-olvin-com` in prod env

## Usage
- This DAG runs as part of the Monthly Update and is externally triggered by `processes-monthly-update-trigger`

## Configuration
- This DAG uses the local config yaml located at: `airflow-dags/dags/processes-monthly-update-agg-stats/config.yaml`

## Troubleshooting
<!-- ### [2024-01-01] -->
- None so far

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [1.0.0] - 2024-05-22
- :tada: DAG documented with new template - [@matias-olvin](https://github.com/matias-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-monthly-update-agg-stats)
- [Confluence Documentation](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2048131073/Aggregated+Statistics)