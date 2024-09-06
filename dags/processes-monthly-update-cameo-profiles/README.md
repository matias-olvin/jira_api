# PROCESSES-MONTHLY-UPDATE-CAMEO-PROFILES

## Introduction

This DAG is triggered after `processes-monthly-update-demographics` in `processes-monthly-update-trigger` has been completed and marked success. 

The input to this process are the `poi_visits_scaled` and the `device_zipcodes_v2` tables. Using static demographics data (`storage-prod-olvin-com.static_demographics_data_v2`) from zipcodes, the table `cameo_staging.cameo_visits` is created and smoothed using moving weighted averages of 24 months. The table `smoothed_transition` is used to create the Postgres tables `SGPlaceCameoRaw` (yearly data) and `SGPlaceCameoMonthlyRaw`. 

A table in the Postgres Database called `cameodemographics` will be used to map the cameo_scores to useful information. 

## Contents
  - [Prerequisites](#prerequisites)
  - [Usage](#usage)
  - [Configuration](#configuration)
  - [Troubleshooting](#troubleshooting)
    <!-- - [\[2024-01-01\]](#2024-01-01) -->
    - None so far
  - [Changelog](#changelog)
    - [\[1.0.2\] - 2024-2-20](#102---2024-2-20)
    - [\[1.0.1\] - 2023-12-18](#101---2023-12-18)
    - [\[1.0.0\] - 2023-9-25](#100---2023-9-25)
  - [References](#references)

## Prerequisites
- `processes-monthly-update-demographics` should have finished and marked success after checking that enough devices are processed. 
- 

## Usage
- This DAG is triggered by the `processes-monthly-update-trigger` DAG after `processes-monthly-update-demographics` has finished. 
- 2 checks are placed after data is INSERTED INTO `storage-prod-olvin-com.cameo_staging.cameo_visits` by the first task. Failing checks could be due of drop/increase in volume from geospatial raw data, errors in `poi_visits_scaled` table or too restrictive thresholds: 
    - `query_check_cameo_visits` looks for anomalies in the scores and/or the locations. Can be marked success if the anomaly is out but close to the thresholds. 
    - `query_check_cameo_visits_postgres`: same checks but focusing on postgres locations only. Can be marked success if the anomaly is out but close to the thresholds. 

## Configuration
- The DAG uses a local config.yaml file

## Troubleshooting
<!-- ### [2024-02-20] -->
- No issues so far

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [1.0.2] - 2024-2-20
- :tada: DAG documented - [@ignacio-olvin](https://github.com/ignacio-olvin)
### [1.0.1] - 2023-12-18
- :mag_right: Change reference period cameo: use latest 30 days - [@carlos-olvin](https://github.com/carlos-olvin)
### [1.0.0] - 2023-9-25
- :mag_right: Restructured DAG - [@kartikeu](https://github.com/kartikeu)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-monthly-update-cameo-profiles)
