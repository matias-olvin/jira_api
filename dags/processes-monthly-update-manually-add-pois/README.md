# PROCESSES-MONTHLY-UPDATE-MANUALLY-ADD-POIS

## Introduction
It is an externally triggered DAG that is used to add manual pois to the following `postgres_batch` tables:
- SGPlaceRaw
- SGPlaceActivity
- SGPlaceHourlyAllVisitsRaw
- SGPlaceHourlyVisitsRaw
- SGPlaceDailyVisitsRaw
- SGPlaceMonthlyVisitsRaw
- SGPlaceMonthlyDemographicRaw
- SGPlaceCameoRaw
- SGPlaceCameoMonthlyRaw
- SGPlaceVisitorBrandDestinations
- SGPlaceVisitorBrandDestinationsPercentages

The manually added pois are also added in SMC permanently but in this DAG the pois are added as a quick fix until the next SMC runs.

## Contents
  - [Prerequisites](#prerequisites)
  - [Usage](#usage)
  - [Configuration](#configuration)
  - [Troubleshooting](#troubleshooting)
    <!-- - [\[2024-01-01\]](#2024-01-01) -->
  - [Changelog](#changelog)
    - [\[1.0.0\] - 2024-01-30](#100---2024-01-30)
  - [References](#references)

## Prerequisites
- requires edit access to the prod and dev google sheets found [here](https://docs.google.com/spreadsheets/d/1hkZZ1jnwh6kk0QO9yQeR3LaXojH2iAnx6omrFaH-M3A/edit#gid=831888214) and [here](https://docs.google.com/spreadsheets/d/1sAUY2nJnn0qz67s1HH3QsdtdhVNoLJwC6kfuYVhuydo/edit#gid=831888214) respectively
- information of pois within 500 metres of the manual poi being added (surrounding pois are used to generate the data)

## Usage
- this DAG is triggered externally by `products-postgres` which is part of the monthly update
- this DAG has 2 manual processes (in chronological order):
    - `check_input_data_in_google_sheets` requires communication between the teams to obtain manual pois that need to be added. Once pois are obtained, insert the pois as rows (with the corresponding data). Note that added_date should be the time that you have inserted that row into the spreadsheet
    - `check_manual_pois_before_insert` is used halt the pipeline before inserting into `postgres_batch` tables. Any final checks such as nulls, correctness of data can be checked here

## Configuration
- this DAG uses `global` config
- `var.value.env_project` needs to be `storage-dev-olvin-com` for testing, or `storage-prod-olvin-com` for production
- google sheet used changes automatically based on the project it runs on

## Troubleshooting
<!-- ### [2024-01-01] -->
- None so far

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [1.0.1] - 2024-04-16
- :four_leaf_clover: Added Verifications/Validations to input info table - [@matias-olvin](https://github.com/matias-olvin)
### [1.0.0] - 2024-01-30
- :tada: DAG documented - [@matias-olvin](https://github.com/matias-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-monthly-update-manually-add-pois)
- [Confluence Documentation](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2378563587/Manually+adding+pois)
