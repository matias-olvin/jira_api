# processes-smc-update-prior-visits

## Introduction

This is a filtering-heuristic approach where we employ expert/user (Pietro & Sam) experience for reasonable values to move brand distributions to a window which is reasonable for the customers and doesn’t show unrealistic values on the platform. The master list for filtering is category based, setting some especially important brands manually and using Sensormatic median value when available.

After this block, no major changes in monitoring variables are expected, as main changes will be on the brands for which no data is available to compare with.

The main idea behind implementing this layer is avoid presenting in the platform clear outliers which trigger red flags in the users when they see it (like banks with 1 customer/day).

From one SMC to the other, new brands appear in SGBrandRaw and need to be added to the prior_brand_visits table.

It first creates the following tables in smc_ground_truth_volume_model dataset:
- `prior_brand_visits`
- `missing_brands`
- `missing_sub_categories`
- `categories_to_append`
- `new_brands`

## Contents
  - [Prerequisites](#prerequisites)
  - [Usage](#usage)
  - [Configuration](#configuration)
  - [Troubleshooting](#troubleshooting)
    - [\[2024-02-13\]](#2024-02-13)
  - [Changelog](#changelog)
    - [\[2.0.0\] - 2024-03-12](#200---2024-03-12)
    - [\[1.0.0\] - 2023-11-24](#100---2023-11-24)
  - [References](#references)

## Prerequisites
- It requires `SGBrandRaw` and `SGPlaceRaw` to be created from the current SMC run (created in `processes-smc-postgres-dynamic`) and `ground_truth_volume_model.prior_brand_visits` from the previous SMC run
- The operator of this pipeline needs access to the [prod](https://docs.google.com/spreadsheets/d/1lkruQBoANmNv3WMIVXZJXJEoki32NSfT_JSO8YJNcD4/edit#gid=831888214) and [dev](https://docs.google.com/spreadsheets/d/15EAEgvo1VQnh5a_AI_akXIx7aBpIAC64v89RQR3RNrc/edit#gid=583706899) google sheets

## Usage
- The DAG is run as part of SMC: it is triggered from processes-smc-block-groundtruth
- The DAG requires manual intervention at several stages:
  - There is a query that checks to see if missing_sub_categories is empty. Here is how to proceed based on the different scenarios:
    - If this task succeeds, there are no categories to append to the prior_brand_visits table and the pipeline will continue as normal
    - If this task fails, it means that there are missing categories that should be accounted for. In the google sheet under worksheet title `categories_to_append-update_vals`, add in the categories from `missing_sub_categories` into the `sub_category` column and for columns `median_category_visits` and `category_median_visits_range`, consult with wider team to obtain reasonable values. `update_at` should be the date the values have been added: note that any rows with a date older than the airflow variable `smc_start_date` will not be used in this pipeline. `fk_sgbrands` should be left empty.
  - When `review_prior_brand_visits_dev_dataset_new_brands_table_via_google_sheets.mark_success_to_confirm_google_sheet_changes` fails, the `new_brands` table will be ready to alter in the google sheet under worksheet title `new_brands-update_vals`. Manually update `median_brand_visits` column with values that have been advised by the experts (if no changes are requested, there is no need for any manipulation of the google sheet). Once the google sheet is ready, mark the task success to continue.
WARNING: Reassure that the new_brands table is the final version before inserting. After this step, the table is inserted into smc_ground_truth_volume_model.prior_brand_visits- the insertion is idempotent for as long as no new_brands appear between insertions

## Configuration
- This DAG uses the global config file
- var.value.env_project needs to be `storage-dev-olvin-com` for testing, or `storage-prod-olvin-com` for production
- The DAG will automatically set the correct datasets based on whether it is being run in `storage-dev-olvin-com` or `storage-prod-olvin-com` -> `prior_brand_visits_dev` for dev and `smc_ground_truth_volume` for prod

## Troubleshooting
### [2024-02-13]
- The task to insert `new_brands` into `prior_brand_visits` was not idempotent due to the statement only deleting (before inserting) rows that had `median_brand_visits` as not null. This worked for the first time because none of the brands existed in the target table (hence new brands), but the second time resulted in duplicate brands because the DELETE statement did not remove the rows with null `median_brand_visits`. This issue has been fixed by deleting and inserting everything in the source from the target.

## Changelog
<!-- start at 1.0.0 (x.y.z) small patches increase z, new features increase y, major changes increase x -->
### [2.0.0] - 2024-03-12
- :sunglasses: DAG now ingests manual data entry from google sheets and no longer uses an intermediate table - [@matias-olvin](https://github.com/matias-olvin)
### [1.0.0] - 2023-11-24
- :magic_wand: DAG created! - [@matias-olvin](https://github.com/matias-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/processes-smc-update-prior-visits)
- [Confluence Documentation](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2319122433/Block+D+Volume+adjust+from+prior+brand+visits)
- [Google Sheets Usage Guide](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2419785742/Google+Sheets+For+Manual+Interventions)