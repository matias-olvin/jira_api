from datetime import datetime
from typing import Union

import numpy as np
from dateutil.relativedelta import relativedelta


def get_backfill_date(input_date: Union[str, datetime]) -> datetime:
    """
    The function takes the input_date and uses the numpy busday_offset function
    to find the first Monday of the following month

    Args:
        input_date (Union[str, datetime]): The date to find the first Monday of the following month

    Returns:
        datetime: The first Monday of the month
    """

    if isinstance(input_date, str):
        input_date = datetime.strptime(input_date, "%Y-%m-%d")

    input_date += relativedelta(months=1)

    return datetime.strptime(
        f"{np.busday_offset(input_date.strftime('%Y-%m'), 0, roll='forward', weekmask='Mon')}",
        "%Y-%m-%d",
    )
