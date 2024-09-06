# SCALING MODELS CREATION TRIGGER

## Introduction

This DAG is manually triggered every 3 months. It is used to trigger the following DAGs:
- processes-smc-sg-places-dynamic
- processes-smc-regressors-dynamic
- processes-smc-poi-visits-backfill
- processes-smc-postgres-dynamic
- poi_matching (in sns project)
- processes-smc
- processes-smc-post-cluster-backfill (in series_smc_start_to_end_date task group)
- processes-smc-places-migration
- processes-smc-regressors-migration
- processes-smc-models-migration
- processes-smc-metrics-migration
- processes-smc-visits-migration
- processes-monthly-update-agg-stats-backfill
- processes-monthly-update-cameo-profiles-backfill

The main purpose of this DAG is to centralise the execution of all DAGs related to the SMC process. Further information on the DAGs that are triggered can be found in the [References](#references) section.

## Contents
  - [Prerequisites](#prerequisites)
  - [Usage](#usage)
  - [Configuration](#configuration)
  - [Troubleshooting](#troubleshooting)
    <!-- - [\[2024-01-01\]](#2024-01-01) -->
  - [Changelog](#changelog)
    - [\[1.0.0\] - 2024-02-27](#100---2024-02-27)
  - [References](#references)

## Prerequisites
- Access is needed to the SNS composer environment and the Source Cloud Google Repo for SNS to check SNS DAGs being triggered from this pipeline and for potential bebugging

## Usage
- This DAG is manually triggered every 3 months.
- Every trigger task group that contains a sensor also contains a `MarkSuccessOperator` at the end to allow for checks after the triggered pipeline has run (the checks are pipeline specific and should be within the documentation). If the checks have passed, then the `MarkSuccessOperator` can be marked success. An example of this is `trigger_poi_visits_backfill` task group.
- Some trigger task groups use a `BashOperator` to trigger a pipeline in SNS. In this case, the `MarkSuccessOperator` is used a manual sensor. This means that the triggered pipeline has to be manually checked in SNS to see if it has finished, if so then mark success to `MarkSuccessOperator`. An example of this is `trigger_poi_matching` task group.

## Configuration
- This DAG uses the global `config.yaml` file
- `var.value.env_project` needs to be `storage-dev-olvin-com` for testing, or `storage-prod-olvin-com` for production

## Troubleshooting
<!-- ### [2024-01-01] -->
- No issues so far

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [1.0.0] - 2024-02-27
- :tada: DAG documented with new layout - [@matias-olvin](https://github.com/matias-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-trigger)
- [Confluence Documentation](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2132180993/Scaling+Models+Creation+Trigger)
- Supplementary Documentation
  - [processes-smc-sg-places-dynamic](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-sg-places-dynamic)
  - [processes-smc-regressors-dynamic](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-regressors-dynamic)
  - [processes-smc-poi-visits-backfill](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-poi-visits-backfill)
  - [processes-smc-postgres-dynamic](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-postgres-dynamic)
  - [poi_matching](https://source.cloud.google.com/sns-vendor-olvin-poc/sns-olvin-data-pipeline/+/master:dags/poi_matching/)
  - [processes-smc](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc)
  - [processes-smc-post-cluster-backfill](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-post-cluster-backfill)
  - [processes-smc-places-migration](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-places-migration)
  - [processes-smc-regressors-migration](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-regressors-migration)
  - [processes-smc-models-migration](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-models-migration)
  - [processes-smc-metrics-migration](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-metrics-migration)
  - [processes-smc-visits-migration](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-visits-migration)
  - [processes-monthly-update-agg-stats-backfill](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-monthly-update-agg-stats)
  - [processes-monthly-update-cameo-profiles-backfill](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-monthly-update-cameo-profiles-backfill)