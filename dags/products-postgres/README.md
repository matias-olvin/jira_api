# PRODUCTS-POSTGRES

## Introduction
This DAG is responsible for preparing/sending postgres tables to the Almanac backend as part of the Monthly Update. It is divided into 2 main sections: `postgres` and `postgres_batch`. The `postgres` dataset can be thought of the staging dataset for `postgres_batch` tables. The `postgres_batch` dataset is modified with data from SNS.

## Contents
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
- [\[2024-03-01\]](#2024-03-01)
- [Changelog](#changelog)
- [\[1.0.0\] - 2024-04-30](#100---2024-04-30)
- [References](#references)

## Prerequisites
- NOT SURE HERE

## Usage
- This DAG is triggered by [processes-monthly-update-trigger](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-monthly-update-trigger)

## Configuration
- This DAG uses a local config file
- For this DAG to function correctly, the trigger for this DAG (see Usage section) needs to pash `update_month` and `update_year` via `dag_run.conf`

## Troubleshooting
### [2024-03-01]
- The invalid keys check failed for specific tables due to the tables no longer being copied over to
`postgres_batch` (the dataset where the checks read from). However, the check for `SGPlaceRaw` table failed due to the check comparing closed and open pois against `SGBrandRaw`: the check should only be comparing open pois. Solution: removed `postgres` only tables from checks, see [pr](https://github.com/olvin-com/airflow-dags/pull/294/files#diff-d08c8ac6720cfa54667e8a17d66e574cd2018b053c1d408bf045efde49442ef3). Also the check on `SGPlaceRaw` now checks pois with `opening_status = 1` only (ie the poi is open), see [pr](https://github.com/olvin-com/airflow-dags/pull/320/files).

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [1.0.0] - 2024-04-30
- :tada: DAG documented with new README.md layout - [@matias-olvin](https://github.com/matias-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/products-postgres)
- [Postgres Data Versioning](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2366177283/Postgres+Data+Versioning)
- [How to recover postgres tables](https://passby.atlassian.net/wiki/spaces/~712020afc07a59232f41acb42b874a27a7c4b9/pages/2355068943/Recovering+postgres)