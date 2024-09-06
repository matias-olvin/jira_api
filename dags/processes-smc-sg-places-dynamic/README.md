# PROCESSES-SMC-SG-PLACES-DYNAMIC

## Introduction

### Brands:

- Runs the tasks to create the `brands_dynamic` table in `smc_sg_places` dataset.
- `brands_dynamic` table is tested for nulls and duplicates in `pid` column.
- These tasks run before `places` tasks because `brands_dynamic` is used to obtain features of pois that are going to be manually added to 
`places_dynamic` (explained below).

### Places:

- Runs the tasks to create the `places_dynamic` table in `smc_sg_places` dataset.
- Additionally:
  - Creates `places_week_array` table in `smc_sg_places` dataset.
  - Updates `site_id_lineage` table in `sg_places` dataset.
  - Manually added pois will be added directly to `places_dynamic`if they haven't already, or if safegraph has sent the poi, in which case that poi will take priority over the manually added poi
- `places_dynamic` table is tested for nulls and duplicates in `pid` column.
- It also contains manual `naics_code` sensitivity checks to update the following tables:
  - `sg_base_tables.sg_categories_match`
  - `sg_base_tables.different_digit_naics_code`
  - `sg_base_tables.naics_code_subcategories`
  - `sg_base_tables.non_sensitive_naics_codes`
  - `sg_base_tables.sensitive_naics_codes`
- These tasks run after `brands` tasks due to dependencies.

## Spend:
- Runs a task to create `smc_sg_places.spend_patterns_static`

## Contents
  - [Prerequisites](#prerequisites)
  - [Usage](#usage)
  - [Configuration](#configuration)
  - [Troubleshooting](#troubleshooting)
    - [\[2024-02-16\]](#2024-02-16)
  - [Changelog](#changelog)
    - [\[1.0.0\] - 2024-02-21](#100---2024-02-21)
  - [References](#references)

## Prerequisites
- The operator of this DAG requires access the google sheet that is used for manual
interventions. The sheets for updating sg_base_tables with new naics codes can be found [here](https://docs.google.com/spreadsheets/d/18BMP1QhTWg7x_S7-HaFQsHicIhecLdocUGGzpb6DjZs/edit#gid=0) for prod and [here](https://docs.google.com/spreadsheets/d/1GkZdWImELNis4i5Jm4OkuiziYM8xqjGqoUToRKmnVlo/edit#gid=0) for dev
- The operator also needs access to the google sheet used for ingesting manually added pois. [Here](https://docs.google.com/spreadsheets/d/1hkZZ1jnwh6kk0QO9yQeR3LaXojH2iAnx6omrFaH-M3A/edit#gid=831888214) is the prod version and [here](https://docs.google.com/spreadsheets/d/1_kq1tfHE8IORV717YrcYNsq838r7gNwRlcW2M1UDEuQ/edit#gid=831888214) is the dev version

## Usage
### Places:
- `places-tasks.update_naics_code_smc_config` gives a chance for the data science team to see whether pois belonging to a specific naics_code (in addition to branded pois) need to be scaled. The query for handling this task can be found in the references section below called Branded Places POIs.
- Ingestion of manually added pois
  - Pois that are added to the sheet mentioned above will be ingested in `ingest_input_table_from_google_sheets_task_group`
  - Note that only pois with `added_date` (the date that the poi is added to the sheet) greater than `var.value.manually_add_pois_deadline_date` and less than `var.value.smc_start_date` will be considered in the process.
  - To ingest the manually added pois that have been added to the google sheet, mark success to `check_input_data_in_google_sheets`
  - If `check_lat_lon_coords_with_region` fails, this means that the longitude and latitude in the input info for a given poi could be incorrect. Please check the coordinates against street address on Google Maps to check the location looks reasonable
  - If `verify_intended_region` fails, it means that the region obtained from a left join with the states table is different from the region obtained from the input info table. To solve the issue, check the region of the poi using the zipcode and street address on Google Maps. If the region is correct in the input info table, the longitude and latitude could be incorrect: if so, follow same proceedure as `check_lat_lon_coords_with_region` and correct the coordinates in the google sheet then re-ingest the table by clearing `ingest_input_table_from_google_sheets_task_group.export_google_sheet_to_gcs_bucket`
- Sensitivity check for new naics codes
  - `new_naics_codes_sensitivity_check` is the table that is exported to a google sheet (link above) containing all the new naics codes in `places_dynamic` (ie do not appear in `non_sensitive_naics_codes` and `sensitive_naics_codes`)
  - The google sheet needs to be reviewed by an expert to assign sensitive (`TRUE`) or non-sensitive (`FALSE`) status to a naics_code in the column called `sensitive`.
  - Mark success to `review_sg_base_tables_staging_dataset_new_naics_codes_sensitivity_check_table_via_google_sheets.mark_success_to_confirm_google_sheet_changes` to confirm changes and proceed to ingestion of naics

## Configuration
- this DAG uses `global` config
- `var.value.env_project` needs to be `storage-dev-olvin-com` for testing, or `storage-prod-olvin-com` for production
- also `var.value.manually_add_pois_deadline_date` needs to be set to copy `var.value.smc_start_date` after smc has finished running.

## Troubleshooting
### [2024-02-16]
- `update_spreadsheet` tasks for `sg_base_tables.sg_categories_match`, `sg_base_tables.different_digit_naics_code`, `sg_base_tables.naics_code_subcategories` failed due to there being no new naics codes to update the tables with. This meant that the table being used to update the google sheet was empty and the tasks threw this error: `DataFrame Empty`. These tasks were marked success.
- Another issue arose from the ingested google sheets not having headers. The solution is to add the column headers to the google sheet, note that the schema must be identical to the schema being defined in the load query for the corresponding google sheet.
- `places-test.places-test-olvin-id-filter` failed because there were branded POIs from last SMC run that are not in the current SMC run. This test could be re-run after running the manual query found in the references section called Branded Places POIs.

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [1.0.1] - 2024-03-15
- :white_check_mark: New checks added to manual input info table from google sheet - [@matias-olvin](https://github.com/matias-olvin)
### [1.0.0] - 2024-02-21
- :tada: DAG documented with new layout - [@matias-olvin](https://github.com/matias-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-sg-places-dynamic)
- [Confluence Documentation](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2094202897/Summary+of+dynamic+places+pipeline)
- [Google Sheets Usage Guide](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2419785742/Google+Sheets+For+Manual+Interventions)
- [Branded Places POIs](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2422046724/SMC+Branded+Places+POIs)
- [Handling Incorrect Polygons in Places Dynamic](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2083913729/Places+dynamic+tenants)
- [Brands Dynamic](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2090565633/Brands+dynamic)
