# COMPOSOSER-WORKFLOWS-DEV-COMPOSER-SETUP

## Introduction
This DAG creates a Cloud Composer Environment in the storage-dev-olvin-com GCP project.

## Contents
- [COMPOSOSER-WORKFLOWS-DEV-COMPOSER-SETUP](#compososer-workflows-dev-composer-setup)
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
- No existing `dev` Composer Environment in `storage-dev-olvin-com`

## Usage
- This DAG is scheduled to run 08:00 (UK time) Monday - Friday.
- It can be manually run if an environment is needed on weekends.

## Configuration
- The local file `../include/bash/composer_settings.sh` is used for configuration.

## Troubleshooting

## Changelog
### [1.0.0] - 2024-01-09
- :tada: DAG documented - [@jake-olvin](https://github.com/jake-olvin)

## References
- [GitHub Link](https://github.com/olvin-com/airflow-dags/tree/main/dags/composer-workflows-dev-composer/setup)
- [Confluence Documentation](https://passby.atlassian.net/wiki/spaces/OLVIN/pages/2213281810/Development+Composer)