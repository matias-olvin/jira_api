# PROCESSES-SMC-BLOCK1-SCALING

## Introduction

This DAG is triggered by `processes-smc-block1` as part of the SMC process. It is responsible for running the geoscaling model and the visits share model. The outputs are inserted into `storage-prod-olvin-com.smc_poi_visits_scaled_block_1.year` where year is the corresponding year for the dates the model using to make the predictions.

## Contents
- [Prerequisites](#prerequisites)
- [DAG Structure](#dag-structure)
- [Usage](#usage)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)
  - [\[2024-02-13\]](#2024-02-13)
- [Changelog](#changelog)
  - [\[1.0.0\] - 2024-05-07](#100---2024-05-07)
- [References](#references)

## Prerequisites
- It requires the visits share and geoscaling model to have been created before making predictions in this DAG. The models are created in `processes-smc-block1`

## Usage
- This DAG is externally triggered by `processes-smc-block1` as part of the SMC process

## Configuration
- This DAG uses the global config file located in the following path: `/dags/common/config.yaml`
- This DAG requires the Airflow variable `smc_start_date` to be set at the start of smc before being triggered: it is used as the last date to run the scaling

## Troubleshooting
### [2024-02-13]
- This DAG is very intensive on the scheduler so it caused the airflow prod env to crash intermittently and for some days to not process. To reduce the load on the scheduler and to fill in the gaps for days that were not processed, portions of dates were run manually by multiple members of the team at the same time. This was the query used (progress can be checked in SMC-Monitoring dashboard found in references):

```sql
DECLARE d DATE DEFAULT CAST('start_date' as DATE);

WHILE d < end_date DO
  CALL storage-prod-olvin-com.procedures.smc_block_1_scaling(
    "storage-prod-olvin-com.smc_poi_visits_scaled_block_1.year",
    CAST(d AS STRING),
    "storage-prod-olvin-com.smc_sg_places.places_filter",
    "storage-prod-olvin-com.static_demographics_data.zipcode_demographics_v2",
    "storage-prod-olvin-com.device_zipcodes",
    "storage-prod-olvin-com.smc_poi_visits_filtered",
    "storage-prod-olvin-com.sg_base_tables.sg_categories_match",
    "storage-prod-olvin-com.sg_base_tables.naics_code_subcategories",
    "storage-prod-olvin-com.smc_sg_places.places_dynamic",
    "storage-prod-olvin-com.regressors.weather",
    "storage-prod-olvin-com.smc_regressors.weather_dictionary",
    "storage-prod-olvin-com.regressors.holidays",
    "storage-prod-olvin-com.smc_regressors.holidays_dictionary",
    "storage-prod-olvin-com.smc_visits_share.tf_classifier",
    "storage-prod-olvin-com.smc_visits_share.prior_distance_parameters",
    "storage-prod-olvin-com.metrics.day_stats_geohits",
    "storage-prod-olvin-com.metrics.day_stats_clusters",
    "storage-prod-olvin-com.metrics.day_stats_visits",
    "storage-prod-olvin-com.smc_geoscaling.geoscaling_model",
    "storage-prod-olvin-com.smc_quality.model_input"
  );
  SET d = DATE_ADD(d, INTERVAL 1 DAY);
END WHILE;
```

where `start_date` is the date to start the loop on (format is `YYYY-MM-DD`), `end_date` is the date to end the loop on (not included in the loop) (format is `YYYY-MM-DD`), and `year` is the name of the table: format is `YYYY` from which is taken from `start_date` and `end_date` (must be equal).

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [1.0.1] - 2024-05-09
- :muscle: replaced dynamic task mapping with a PythonOperator to send scaling queries via api. This was done to avoid overloading scheduler CPU- [@matias-olvin](https://github.com/matias-olvin)
### [1.0.0] - 2024-05-07
- :tada: DAG documented with new layout- [@matias-olvin](https://github.com/matias-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-block1-scaling)
- [Visits Share Docs](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/1921581078/Visits+Share)
- [Geoscaling Docs](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2176548865/Geoscaling)
- [SMC-Monitoring Dashboard](https://lookerstudio.google.com/reporting/7f5ea8cf-0559-473b-b1b6-d053b48a6bf4/page/p_g2a8up2cbd). Look under Partition view: smc_poi_visits_scaled_block_1.