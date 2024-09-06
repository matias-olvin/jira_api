from __future__ import annotations

from typing import Union

from airflow.models import DAG, TaskInstance
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup


def register(start: Union[TaskInstance, TaskGroup], dag: DAG) -> TaskGroup:
    """
    Register tasks on the dag

    Args:
        start (airflow.models.TaskInstance): Task instance all tasks will
        be registered downstream from.

    Returns:
        airflow.utils.task_group.TaskGroup: The sample finance task group.
    """
    database = "passby_data"
    warehouse = "compute_wh_med"
    sensor = ExternalTaskSensor(
        task_id="sample_placekey_sensor",
        external_dag_id="{{ params['sample_feed'] }}",
        external_task_id="end",
        execution_date_fn=lambda dt: dt.replace(
            day=1,
            hour=6,
        ),
        mode="reschedule",
        poke_interval=60 * 5,
        timeout=60 * 60 * 24,
    )

    start >> sensor

    with TaskGroup(group_id="general-sample") as general_sample:

        schema = "public_data_feeds_sample"

        clear_snowflake_placekeys = SnowflakeOperator(
            database=database,
            task_id="clear_snowflake_placekeys",
            sql=f"TRUNCATE TABLE {database}.{schema}.placekeys;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        refresh_snowflake_placekeys = SnowflakeOperator(
            database=database,
            task_id="refresh_snowflake_placekeys",
            sql=f"ALTER STAGE {database}.{schema}.ext_stage_sample_general_placekeys REFRESH ;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        copy_snowflake_placekeys = SnowflakeOperator(
            database=database,
            task_id="copy_snowflake_placekeys",
            sql=f"""USE WAREHOUSE {warehouse}; COPY INTO {database}.{schema}.placekeys
  FROM @ext_stage_sample_general_placekeys
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
  PATTERN='.*[.]parquet';""",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        clear_snowflake_store_visitors = SnowflakeOperator(
            database=database,
            task_id="clear_snowflake_store_visitors",
            sql=f"TRUNCATE TABLE {database}.{schema}.store_visitors;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        refresh_snowflake_store_visitors = SnowflakeOperator(
            database=database,
            task_id="refresh_snowflake_store_visitors",
            sql=f"ALTER STAGE {database}.{schema}.ext_stage_sample_general_store_visitors REFRESH ;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        copy_snowflake_store_visitors = SnowflakeOperator(
            database=database,
            task_id="copy_snowflake_store_visitors",
            sql=f"""USE WAREHOUSE {warehouse}; COPY INTO {database}.{schema}.store_visitors
  FROM @ext_stage_sample_general_store_visitors
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
  PATTERN='.*[.]parquet';""",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        clear_snowflake_store_visits_trend = SnowflakeOperator(
            database=database,
            task_id="clear_snowflake_store_visits_trend",
            sql=f"TRUNCATE TABLE {database}.{schema}.store_visits_trend;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        refresh_snowflake_store_visits_trend = SnowflakeOperator(
            database=database,
            task_id="refresh_snowflake_store_visits_trend",
            sql=f"ALTER STAGE {database}.{schema}.ext_stage_sample_general_store_visits_trend REFRESH ;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        copy_snowflake_store_visits_trend = SnowflakeOperator(
            database=database,
            task_id="copy_snowflake_store_visits_trend",
            sql=f"""USE WAREHOUSE {warehouse}; COPY INTO {database}.{schema}.store_visits_trend
  FROM @ext_stage_sample_general_store_visits_trend
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
  PATTERN='.*[.]parquet';""",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        clear_snowflake_store_visits = SnowflakeOperator(
            database=database,
            task_id="clear_snowflake_store_visits",
            sql=f"TRUNCATE TABLE {database}.{schema}.store_visits;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        refresh_snowflake_store_visits = SnowflakeOperator(
            database=database,
            task_id="refresh_snowflake_store_visits",
            sql=f"ALTER STAGE {database}.{schema}.ext_stage_sample_general_store_visits REFRESH ;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        copy_snowflake_store_visits = SnowflakeOperator(
            database=database,
            task_id="copy_snowflake_store_visits",
            sql=f"""USE WAREHOUSE {warehouse}; COPY INTO {database}.{schema}.store_visits
  FROM @ext_stage_sample_general_store_visits
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
  PATTERN='.*[.]parquet';""",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        (
            [
                clear_snowflake_placekeys,
                clear_snowflake_store_visitors,
                clear_snowflake_store_visits,
                clear_snowflake_store_visits_trend,
            ]
            >> refresh_snowflake_placekeys
            >> refresh_snowflake_store_visitors
            >> refresh_snowflake_store_visits
            >> refresh_snowflake_store_visits_trend
            >> copy_snowflake_placekeys
            >> copy_snowflake_store_visitors
            >> copy_snowflake_store_visits
            >> copy_snowflake_store_visits_trend
        )

    with TaskGroup(group_id="finance-sample") as finance_sample:

        schema = "public_data_feeds_sample_finance"

        clear_snowflake_placekeys = SnowflakeOperator(
            database=database,
            task_id="clear_snowflake_placekeys",
            sql=f"TRUNCATE TABLE {database}.{schema}.placekeys;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        refresh_snowflake_placekeys = SnowflakeOperator(
            database=database,
            task_id="refresh_snowflake_placekeys",
            sql=f"ALTER STAGE {database}.{schema}.ext_stage_sample_finance_placekeys REFRESH ;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        copy_snowflake_placekeys = SnowflakeOperator(
            database=database,
            task_id="copy_snowflake_placekeys",
            sql=f"""USE WAREHOUSE {warehouse}; COPY INTO {database}.{schema}.placekeys
  FROM @ext_stage_sample_finance_placekeys
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
  PATTERN='.*[.]parquet';""",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        clear_snowflake_store_visitors = SnowflakeOperator(
            database=database,
            task_id="clear_snowflake_store_visitors",
            sql=f"TRUNCATE TABLE {database}.{schema}.store_visitors;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        refresh_snowflake_store_visitors = SnowflakeOperator(
            database=database,
            task_id="refresh_snowflake_store_visitors",
            sql=f"ALTER STAGE {database}.{schema}.ext_stage_sample_finance_store_visitors REFRESH ;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        copy_snowflake_store_visitors = SnowflakeOperator(
            database=database,
            task_id="copy_snowflake_store_visitors",
            sql=f"""USE WAREHOUSE {warehouse}; COPY INTO {database}.{schema}.store_visitors
  FROM @ext_stage_sample_finance_store_visitors
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
  PATTERN='.*[.]parquet';""",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        clear_snowflake_store_visits_trend = SnowflakeOperator(
            database=database,
            task_id="clear_snowflake_store_visits_trend",
            sql=f"TRUNCATE TABLE {database}.{schema}.store_visits_trend;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        refresh_snowflake_store_visits_trend = SnowflakeOperator(
            database=database,
            task_id="refresh_snowflake_store_visits_trend",
            sql=f"ALTER STAGE {database}.{schema}.ext_stage_sample_finance_store_visits_trend REFRESH ;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        copy_snowflake_store_visits_trend = SnowflakeOperator(
            database=database,
            task_id="copy_snowflake_store_visits_trend",
            sql=f"""USE WAREHOUSE {warehouse}; COPY INTO {database}.{schema}.store_visits_trend
  FROM @ext_stage_sample_finance_store_visits_trend
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
  PATTERN='.*[.]parquet';""",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        clear_snowflake_store_visits = SnowflakeOperator(
            database=database,
            task_id="clear_snowflake_store_visits",
            sql=f"TRUNCATE TABLE {database}.{schema}.store_visits;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        refresh_snowflake_store_visits = SnowflakeOperator(
            database=database,
            task_id="refresh_snowflake_store_visits",
            sql=f"ALTER STAGE {database}.{schema}.ext_stage_sample_finance_store_visits REFRESH ;",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        copy_snowflake_store_visits = SnowflakeOperator(
            database=database,
            task_id="copy_snowflake_store_visits",
            sql=f"""USE WAREHOUSE {warehouse}; COPY INTO {database}.{schema}.store_visits
  FROM @ext_stage_sample_finance_store_visits
  FILE_FORMAT = (TYPE = 'PARQUET')
  MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE'
  PATTERN='.*[.]parquet';""",
            snowflake_conn_id="snowflake_conn",
            schema=schema,
        )

        (
            [
                clear_snowflake_placekeys,
                clear_snowflake_store_visitors,
                clear_snowflake_store_visits,
                clear_snowflake_store_visits_trend,
            ]
            >> refresh_snowflake_placekeys
            >> refresh_snowflake_store_visitors
            >> refresh_snowflake_store_visits
            >> refresh_snowflake_store_visits_trend
            >> copy_snowflake_placekeys
            >> copy_snowflake_store_visitors
            >> copy_snowflake_store_visits
            >> copy_snowflake_store_visits_trend
        )

    sensor >> general_sample >> finance_sample
    return finance_sample
