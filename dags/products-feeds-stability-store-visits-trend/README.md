# DAG ID: data_feed_store_visits_trend

## Description
This DAG loads data into a BigQuery table named store_visits_trend and performs data quality checks and data tweaks.

## DAG Configuration

### Default Arguments
The DAG is configured with the following default arguments:
- **start_date**: September 16, 2022
- **depends_on_past**: True
- **wait_for_downstream**: True
- **retries**: 3
- **max_active_runs**: 1
- **on_failure_callback**: Sends a Slack message to the user with the ID `U034YDXAD1R` when a task fails

### Schedule
The DAG is scheduled to run at 6:00 AM on the first day of every month.

### Parameters
The DAG is loaded with parameters from `config.yaml`.

## DAG Tasks
- **start**: This is a `DummyOperator` task with the task_id *"start"*. It is the starting point of the DAG.
- **truncate_store_visits_trend**: This task truncates the `store_visits_trend` table in BigQuery. It is a `BigQueryInsertJobOperator` task with the task_id *"truncate_store_visits_trend"*. It uses a SQL query stored in the `bigquery/truncate_table.sql` file.
- **insert_store_visits_trend**: This task inserts data into the `store_visits_trend` table in BigQuery. It is a `BigQueryInsertJobOperator` task with the task_id *"insert_store_visits_trend"*. It uses a SQL query stored in the `bigquery/store_visits_trend.sql` file.
- **data_quality_checks_start** and **data_quality_checks_end**
These are `DummyOperator` tasks with the task_ids *"data_quality_checks_start"* and *"data_quality_checks_end"*, respectively. They are used to group the data quality check tasks together.
- **Data Quality Checks**: This section loops through all the SQL files in the `bigquery/data_quality` folder and performs data quality checks on the `store_visits_trend` table. Each SQL file contains a single data quality check. The SQL queries are executed using `BigQueryValueCheckOperator` tasks.
- **data_tweaks_start** and **data_tweaks_end**: These are `DummyOperator` tasks with the task_ids *"data_tweaks_start"* and *"data_tweaks_end"*, respectively. They are used to group the data tweak tasks together.
- **Data Tweaks**: This section loops through all the SQL files in the `bigquery/tweaks` folder and performs data tweaks on the `store_visits_trend` table. Each SQL file contains a single data tweak. The SQL queries are executed using `BigQueryInsertJobOperator` tasks. A `PythonOperator` task is used to check the status of the data quality check task that the data tweak is fixing. If the data quality check task is successful, the data tweak task is skipped.