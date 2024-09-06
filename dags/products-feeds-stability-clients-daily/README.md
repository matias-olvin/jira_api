# PRODUCTS-FEEDS-STABILITY-CLIENTS-DAILY

## Introduction
This DAG is responsible for sending the daily feeds to clients. It is scheduled to run every day at 11am UTC and sends daily data from 
`storage-prod-olvin-com.public_data_feeds_type_2` or `storage-prod-olvin-com.public_data_feeds_type_2_finance` if the client has asked for finance feed.

## Contents
  - [Prerequisites](#prerequisites)
  - [DAG Structure](#dag-structure)
  - [Usage](#usage)
  - [Configuration](#configuration)
  - [Troubleshooting](#troubleshooting)
    <!-- - [\[2024-01-01\]](#2024-01-01) -->
  - [Changelog](#changelog)
    - [\[1.0.0\] - 2024-04-30](#100---2024-04-30)
  - [References](#references)

## Prerequisites
- This DAG requires the generation DAGs to have run succesfully before sending data to clients (see references for more detail)
- It also requires the correct permissions in order to send data to client buckets (if applicable)

## Usage
- This DAG is scheduled to run every day at 11am UTC

## Configuration
- This DAG uses a local config file: `./dags/products-feeds-stability-clients-daily/config.yaml`
- The data frequency of the daily feed is dependent on the `first_monday` airflow variable being set at the start of this pipeline (done automatically).
It also relies on `smc_end_date_prev` airflow variable being set/updated when the latest smc has finished (done automatically within the smc pipeline).
Note: data frequency is either `daily` or `historical`, see references for explanation.

## Troubleshooting
<!-- ### [2024-01-01] -->
- None so far

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [1.0.0] - 2024-04-30
- :tada: DAG created - [@matias-olvin](https://github.com/matias-olvin)

## References
- [Delivery Methods Details](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2437054598/Data+transfer+to+clients)
- [Generation of data source for Feeds](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2459828226/Data+Feeds+Generation)
- [Transfer DAGs Docs](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2465792002/Data+Feeds+Transfer)
- [Daily feed implementation notes](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2500558849/Daily+Feeds+Implementation)