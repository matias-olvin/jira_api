from common import vars


def parse_full_table(table: str, include_dataset: bool = False) -> str:
    """
    Takes a fully qualified table name (`project.dataset.table`) and returns the table
    name in the format `dataset.table` or just `table` depending on the value of the
    `include_dataset` parameter

    Args:
      table (str): The name of the table to be queried.
      include_dataset (bool): If True, the dataset name will be included in the table
    name. Defaults to False

    Returns:
      The table name without the project name.
    """
    _, _dataset, _table = table.split(".")
    return f"{_dataset}.{_table}" if include_dataset else _table


def accessible_table_name_format(accessible_by: str, table: str, dataset: str = None) -> str:
    """
    Formats a table name for accessible datasets based on the accessible_by parameter.

    Args:
      accessible_by (str): Specifies who the table is accessible by. It can have two
    possible values: "sns" or "olvin".
      table (str): Represents the name of a table. It is expected to be in the format
    `project.dataset.table`.

    Returns:
      a string that represents the table name for accessible_by dataset.
    """
    if dataset:
        _dataset = dataset
        _table = table
    else:
        _, _dataset, _table = table.split(".")

    if accessible_by == "sns":
        return f"{vars.OLVIN_PROJECT}.{vars.ACCESSIBLE_BY_SNS}.{_dataset}-{_table}"
    elif accessible_by == "olvin":
        return f"{vars.SNS_PROJECT}.{vars.ACCESSIBLE_BY_OLVIN}.{_dataset}-{_table}"