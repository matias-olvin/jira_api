from datetime import date
from typing import Dict, List, Optional


def _convert_filter_by_to_where(
    filter_by: Optional[Dict[str, str or int or float or bool]] or None
) -> str:
    """
    Takes a dictionary of column names and values, and returns a string that can be used in a SQL
    query to filter the results

    Args:
      filter_by (Optional[Dict[str, str or int or float or bool]] or None): A dictionary of column names and values to
    filter by.

    Returns:
      A string that is a WHERE clause for a SQL query.
    """
    for col, val in filter_by.items():
        if isinstance(val, str):
            try:
                date.fromisoformat(val)
                filter_by[col] = f"DATE('{val}')"
            except ValueError:
                filter_by[col] = f"'{val}'"

    return (
        "WHERE\n\t"
        + "\n\tAND ".join(f"{col} = {val}" for col, val in filter_by.items())
        if filter_by is not None
        else ""
    )


def delete_from_table(
    table: str, filter_by: Optional[Dict[str, str or int or float or bool]]
) -> str:
    """
    It takes a table name and a filter_by dictionary and returns a SQL DELETE statement

    Args:
      table (str): The name of the table you want to delete from.
      filter_by (Optional[Dict[str, str or int or float or bool]]): A dictionary of column names and values to filter by.

    Returns:
      A string.
    """
    delete = f"DELETE FROM `{table}`"  # backticks needed around tables.
    where = _convert_filter_by_to_where(filter_by=filter_by)

    return f"{delete}\n{where};"


def write_copy(source: str, destination: str) -> str:
    """
    Write a query that creates a table the same as the source table using `COPY`.

    Args:
      source (str): the name of the table you want to copy.
      destination (str): The name of the table you want to create.

    Returns:
      A string that is a query to create a table.
    """
    return f"CREATE OR REPLACE TABLE `{destination}` \n COPY `{source}`;"  # backticks needed around tables.


def write_append(
    source: str,
    destination: str,
    filter_by: Optional[Dict[str, str or int or float or bool]] = None,
) -> str:
    """
    Write a query that inserts from the source table into the destination table.

    Args:
      source (str): the table to copy from
      destination (str): The table to write to.
      filter_by (Optional[Dict[str, str or int or float or bool]]): A dictionary with the column to filter on as the key
       and a list containing the value to filter and the SQL data type as the value.
    by.

    Returns:
      A string that is a query to insert into a table.
    """
    # delete = delete_from_table(table=destination, filter_by=filter_by)  # delete any existing data for idempotency
    insert = f"INSERT INTO `{destination}`"  # backticks needed around tables.
    select = f"SELECT * \nFROM `{source}`"  # backticks needed around tables.
    where = _convert_filter_by_to_where(filter_by=filter_by)

    # return f"{delete}\n\n{insert}\n{select}\n{where};"
    return f"{insert}\n{select}\n{where};"


def copy_table(source: str, destination: str) -> str:
    """
    Copy source table to the destination table in BigQuery.

    Args:
      source (str): Source table (`project.dataset.table`) to copy from.
      destination (str): Destination table (`project.dataset.table`) to write to.

    Returns:
      a string that represents a BigQuery SQL query.
    """
    return f"CREATE OR REPLACE TABLE `{destination}`\nCOPY `{source}`;\n"


def drop_table(table: str) -> str:
    """
    Drop table in BigQuery.

    Args:
      table (str): Table (`project.dataset.table`) to drop.

    Returns:
      a string that represents a BigQuery SQL query.
    """
    return f"DROP TABLE `{table}`;\n"


def max_row_count_check(table: str, max_row_count: int) -> str:
    """
    Assert a maximum number of rows for a table in BigQuery.

    Args:
      table (str): Table (`project.dataset.table`) to drop.

    Returns:
      a string that represents a BigQuery SQL query.
    """
    return f"""
  ASSERT (
    SELECT COUNT(*)
    FROM {table}
  ) < {max_row_count} AS 'The number of rows exceeds the max row count of {max_row_count} in table {table}';
  """

def start_dag_timer_query(dag_id: str) -> str:
  """
  Start a timer for a DAG in BigQuery.

  Args:
    dag_id (str): The DAG ID that is being timed.

  Returns:
    a string that represents a BigQuery SQL query.
  """

  query = f"""

  ASSERT (
    SELECT COUNT(*)
    FROM `{{{{ var.value.env_project }}}}.dag_metrics.dag_timer`
    WHERE dag_id LIKE "{dag_id}" AND parent_dag_id LIKE "{{{{ dag.dag_id.lower()[:63] |  replace('.','-') }}}}"
    AND execution_date = DATETIME("{{{{ ds }}}}")
  ) = 0 AS "DagRun with dag_id: {dag_id}, parent_dag_id: '{{{{ dag.dag_id.lower()[:63] |  replace('.','-') }}}}', and execution_date: DATETIME('{{{{ ds }}}}') already being/been timed: check {{{{ var.value.env_project }}}}.dag_metrics.dag_timer for details";

  CREATE TEMP TABLE dag_to_be_timed AS (
    SELECT "{dag_id}" AS dag_id, "{{{{ dag.dag_id.lower()[:63] |  replace('.','-') }}}}" AS parent_dag_id, DATETIME("{{{{ ds }}}}") AS execution_date
  );

  MERGE INTO `{{{{ var.value.env_project }}}}.dag_metrics.dag_timer` AS target
  USING dag_to_be_timed AS source
  ON target.dag_id = source.dag_id AND target.parent_dag_id = source.parent_dag_id AND target.execution_date = source.execution_date
  WHEN NOT MATCHED THEN
    INSERT (dag_id, parent_dag_id, execution_date, start_date, completed)
    VALUES (source.dag_id, source.parent_dag_id, source.execution_date, CURRENT_TIMESTAMP, FALSE);
  """

  return query


def end_dag_timer_query(dag_id: str) -> str:
  """
  End a timer for a DAG in BigQuery.

  Args:
    dag_id (str): The DAG ID that is being timed.

  Returns:
    a string that represents a BigQuery SQL query.
  """

  query = f"""

  ASSERT (
    SELECT COUNT(*)
    FROM `{{{{ var.value.env_project }}}}.dag_metrics.dag_timer`
    WHERE dag_id LIKE "{dag_id}" AND parent_dag_id LIKE "{{{{ dag.dag_id.lower()[:63] |  replace('.','-') }}}}"
    AND execution_date = DATETIME("{{{{ ds }}}}") AND completed IS TRUE
  ) = 0 AS "DagRun with dag_id: {dag_id}, parent_dag_id: '{{{{ dag.dag_id.lower()[:63] |  replace('.','-') }}}}', and execution_date: DATETIME('{{{{ ds }}}}') has already been timed or timer has not started: check {{{{ var.value.env_project }}}}.dag_metrics.dag_timer for details";

  UPDATE `{{{{ var.value.env_project }}}}.dag_metrics.dag_timer`
  SET end_date = CURRENT_TIMESTAMP, completed = TRUE
  WHERE dag_id LIKE "{dag_id}" AND parent_dag_id LIKE "{{{{ dag.dag_id.lower()[:63] |  replace('.','-') }}}}"
  AND execution_date = DATETIME("{{{{ ds }}}}") AND completed IS FALSE;
  """

  return query
