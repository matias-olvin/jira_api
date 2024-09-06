# PROCESSES-DAILY-VISITS-ESTIMATION

## Introduction
This creates 2 DAGs:
- processes-daily-visits-estimation-staging
- processes-daily-visits-estimation-production

These DAGs are responsible for re-estimating visits with the latest SNS ground truth data.

## Contents
- [PROCESSES-DAILY-VISITS-ESTIMATION](#processes-daily-visits-estimation)
  - [Introduction](#introduction)
  - [Contents](#contents)
  - [Prerequisites](#prerequisites)
    - [Production](#production)
    - [Staging](#staging)
  - [Usage](#usage)
    - [Production](#production-1)
    - [Staging](#staging-1)
  - [Configuration](#configuration)
  - [Troubleshooting](#troubleshooting)
  - [Changelog](#changelog)
    - [\[1.0.0\] - 2024-04-15](#100---2024-04-15)

## Prerequisites
### Production
- Requires `supervised_visits_estimation` model in MLflow Model Registry with alias `@production`.
- Requires table `storage-prod-olvin-com.accessible_by_olvin_almanac.postgres_rt-SGPlaceDailyVisitsRaw`.
### Staging
- Requires `supervised_visits_estimation` model in MLflow Model Registry with alias `@staging`.
- Requires table `storage-prod-olvin-com.accessible_by_olvin.postgres_rt-SGPlaceDailyVisitsRaw`.

## Usage
### Production
- Runs daily @ 06:00 AM (Europe/London), should run everyday.
### Staging
- Runs daily @ 06:00 AM (Europe/London), will skip if no staging, will retry (for 10 days) if table not found.

## Configuration
- Both DAGs use local config file (`./config.yaml`)

## Troubleshooting
- None

## Changelog
### [1.0.0] - 2024-04-15
- :tada: DAG created - [@jake-olvin](https://github.com/jake-olvin)