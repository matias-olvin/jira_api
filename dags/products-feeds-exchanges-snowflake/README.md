# DAG ID: products-feeds-exchanges-snowflake

## Description
This DAG loads data into Snowflake tables that are shared in the Snowflake listings.

## DAG Configuration

### Default Arguments
The DAG is configured with the following default arguments:
- **start_date**: September 16, 2022
- **depends_on_past**: True
- **wait_for_downstream**: True
- **retries**: 3
- **max_active_runs**: 1
- **on_failure_callback**: Sends a Slack message to Kartikey and Matias when a task fails.

### Schedule
The DAG is scheduled to run at 10:00 AM on the first day of every month.

### Parameters
The DAG is loaded with parameters from `config.yaml`.

## DAG Tasks
- **start**: This is a `EmptyOperator` task with the task_id *"start"*. It is the starting point of the DAG.
- **general-sample**: This task group truncates all tables in the `public_data_feeds_sample` schema in SnowFlake. It then adds data to each of the truncated tables from predefined GCS files.
- **finance-sample**: This task group truncates all tables in the `public_data_feeds_sample_finance` schema in SnowFlake. It then adds data to each of the truncated tables from predefined GCS files.
- **general-type1**: This task group truncates all tables in the `public_data_feeds_type_1` schema in SnowFlake. It then adds data to each of the truncated tables from predefined GCS files.
- **finance-type1**: This task group truncates all tables in the `public_data_feeds_type_1_finance` schema in SnowFlake. It then adds data to each of the truncated tables from predefined GCS files.
- **general-type2**: This task group truncates all tables in the `public_data_feeds_type_2` schema in SnowFlake. It then adds data to each of the truncated tables from predefined GCS files.
- **finance-type2**: This task group truncates all tables in the `public_data_feeds_type_2_finance` schema in SnowFlake. It then adds data to each of the truncated tables from predefined GCS files.