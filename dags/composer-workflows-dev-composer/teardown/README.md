# COMPOSOSER-WORKFLOWS-DEV-COMPOSER-TEARDOWN

## Introduction
This DAG destroys a Cloud Composer Environment in the storage-dev-olvin-com GCP project, backing up the dags, variables, connections, and dependencies in Cloud Storage.

## Contents
- [COMPOSOSER-WORKFLOWS-DEV-COMPOSER-TEARDOWN](#compososer-workflows-dev-composer-teardown)
  - [Introduction](#introduction)
  - [Contents](#contents)
  - [Prerequisites](#prerequisites)
  - [Usage](#usage)
  - [Configuration](#configuration)
  - [Troubleshooting](#troubleshooting)
  - [Changelog](#changelog)
    - [\[1.0.0\] - 2024-01-09](#100---2024-01-09)
  - [References](#references)

## Prerequisites
- An existing `dev` Composer Environment in `storage-dev-olvin-com`

## Usage
- This DAG is scheduled to run 19:15 (UK time) Monday - Friday.
- It sends a Slack message, then waits 15-minutes before tearing down the environment.
- If teardown should be stopped, then pause the DAG before the 15-minutes elapses, then unpause when you are finished working in the Composer environment.
- The DAG can be manually run if an environment needs to be torn down on weekends.

## Configuration
- The local file `../include/bash/composer_settings.sh` is used for configuration.

## Troubleshooting

## Changelog
### [1.0.0] - 2024-01-09
- :tada: DAG documented - [@jake-olvin](https://github.com/jake-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/composer-workflows-dev-composer/teardown)
- [Confluence Documentation](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2213281810/Development+Composer)