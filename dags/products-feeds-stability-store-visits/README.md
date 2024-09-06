# PRODUCTS-FEEDS-STABILITY-STORE-VISITS

## Introduction
This DAG creates the base table called `store_visits` (weekly table) as part of the data feeds type 2 products. It uses said table to create `store_visits_daily` 
(daily table). The daily table is used in the daily feeds and the weekly table is used in the monthly/weekly feeds.

## Contents
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
<!-- - [\[2024-01-01\]](#2024-01-01) -->
- [Changelog](#changelog)
- [\[1.0.0\] - 2024-05-29](#100---2024-05-29)
- [References](#references)

## Prerequisites
- This DAG requires the latest MU to be completed. This step is automated using a sensor at the start of the DAG that waits for
the task `start_sending_to_backend` in DAG `products-postgres`

## Usage
- This DAG is scheduled run on the first of every month
- If any of the final base tables need to be recreated, ensure the corresponding export tasks are rerun as well. For example `data-feed-export-backfill-finance-create-table`
requires `data-feed-export-backfill-finance` task group to be re-run (ensure dataproc is re-created, if deleted, by rerunning `create_export_to_gcs_cluster`)

## Configuration
- This DAG uses the local config found here: `airflow-dags/dags/products-feeds-stability-store-visits/config.yaml`
- The airflow variable `gcs_dags_folder` needs to be set manually to the path to the dags folder in the Composer env
- The variables `data_feed_data_version` and `data_feed_places_version` are set automatically in this DAG

## Troubleshooting
<!-- ### [2024-01-01] -->
- None so far

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [1.0.0] - 2024-05-29
- :tada: Moved final table creation tasks before dataproc creation. README new layout - [@matias-olvin](https://github.com/matias-olvin)
## References

- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/products-feeds-stability-store-visits)
- [Data Feeds Generation](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2459828226/Data+Feeds+Generation)
- [Data Feeds Methodology](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2373287947/Data+Feeds+methodology)
