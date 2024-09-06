# PRODUCTS-MARKETING-POSTGRES-WEEKLY

TO BE COMPLETED

## Introduction
The purpose of this DAG is to periodically check the following tables for potentially sensitive naics codes:
- `postgres_batch.SGBrandRaw`
- `postgres.SGBrandRaw`
- `postgres_batch.SGPlaceRaw`
- `postgres.SGPlaceRaw`

The naics codes of the tables are checked to see if they do not appear in `sg_base_tables.non_sensitive_naics_codes`. If they do not appear, they could be sensitive.

IMPORTANT NOTE: there is a chance that a naics code could be incorrectly identified as non-sensitive: this means a sensitive naics code could pass the assertion and go undetected.

## Contents
- [Prerequisites](#prerequisites)
- [Usage](#usage)
- [Configuration](#configuration)
- [Troubleshooting](#troubleshooting)<!-- - [\[2024-01-01\]](#2024-01-01) -->
- [Changelog](#changelog)
- [\[1.0.0\] - 2024-03-05](#100---2024-03-05)
- [References](#references)

## Prerequisites
- This DAG requires `sg_base_tables.non_sensitive_naics_codes` to be the latest version from when new naics codes are ingested and are [manually determined](https://github.com/olvin-com/airflow-dags/blob/main/dags/processes-smc-sg-places-dynamic/steps/naics_codes/update_new_naics_codes_sensitivity_check.py) to be sensitive or non-sensitive

## Usage
- This DAG is scheduled to run at 10AM everyday and does not require any manual intervention
- If a check fails, run the following query to obtain the naics code then immediately notify the wider team:
```
SELECT
        naics_code
    FROM
        `storage-prod-olvin-com.name_of_dataset.name_of_table_that_failed_check`
    WHERE
        naics_code NOT IN (
            SELECT
                naics_code
            FROM
                `storage-prod-olvin-com.sg_base_tables.non_sensitive_naics_codes`
        )
```

## Configuration
- This DAG uses the `global` config file
- `var.value.env_project` needs to be `storage-dev-olvin-com` for testing, or `storage-prod-olvin-com` for production

## Troubleshooting
<!-- ### [2024-01-01] -->
- No issues so far

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [1.0.0] - 2024-03-05
- :tada: DAG documented - [@matias-olvin](https://github.com/matias-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/products-postgres-daily-asserts)
- [New Naics Codes Check Documentation](https://github.com/olvin-com/airflow-dags/blob/main/dags/processes-smc-sg-places-dynamic/README.md#places-1)