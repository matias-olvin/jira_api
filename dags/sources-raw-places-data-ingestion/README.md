# SOURCES-RAW-PLACES-DATA-INGESTION

## Introduction
This DAG is designed to load the monthly Safegraph data from GCS and update `sg_places_staging.places_history` and `sg_places_staging.brands_history` with the latest places and brands data. `olvin_id`'s are then created and updated using the Safegraph Lineage API.

## Contents
- [SOURCES-RAW-PLACES-DATA-INGESTION](#sources-raw-places-data-ingestion)
  - [Introduction](#introduction)
  - [Contents](#contents)
  - [Prerequisites](#prerequisites)
  - [DAG Structure](#dag-structure)
  - [Usage](#usage)
  - [Configuration](#configuration)
  - [Troubleshooting](#troubleshooting)
    - [Ongoing](#ongoing)
  - [Changelog](#changelog)
    - [\[1.0.0\] - 2024-01-02](#100---2024-01-02)
  - [References](#references)

## Prerequisites
- This DAG runs the latest version of the `europe-west1-docker.pkg.dev/storage-prod-olvin-com/apis/lineage` image, ensure the image is up to date before triggering.
- No extra packages are required for running this DAG.

## DAG Structure
![SVG Image](diagrams/dag.svg "SVG Image")

## Usage
- This DAG is scheduled to run on the 8th of the month.

## Configuration
- This DAG uses a local `config.yaml` file and the Airflow variables.

## Troubleshooting
### Ongoing
- There is a known error in the Safegraph data for the brand `SG_BRAND_5a8c6560d880a0ce`, where both `Seaworld Parks and Entertainment` and `seaworld parks and entertainment` are used as the `name`. This breaks the merge condition in task `upsert_places_to_brands_history`. To resolve, run the following query in BigQuery, then clear the task.

```sql
UPDATE `storage-prod-olvin-com.sg_places_staging.sg_places`
SET 
    brands = 'Seaworld Parks and Entertainment'
WHERE 
    fk_sgbrands = 'SG_BRAND_5a8c6560d880a0ce'
```

## Changelog
### [1.0.0] - 2024-01-02
- :tada: DAG documented - [@jake-olvin](https://github.com/jake-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/sources-raw-places-data-ingestion)
- [Confluence Documentation](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2376728631/sources-raw-places-data-ingestion)
- [POI Monitoring Dashboard](https://lookerstudio.google.com/u/1/reporting/b8f80a21-0337-4f63-840a-9cfda260812d/page/7UusC)
- [Safegraph Documentation](https://docs.safegraph.com/docs)
- [Lineage API Documentation](https://docs.placekey.io/#93f568b4-aaa2-4953-a2f3-7968371109a8)
- [Lineage API Dashboard](https://dev.placekey.io/default/dashboard)