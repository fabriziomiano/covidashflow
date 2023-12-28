import datetime as dt

import pandas as pd
from settings.vars import VAX_DATE_KEY
from utils.common import get_logger

logger = get_logger("OD-extract")


def get_od_df(url, do_parse_dates=False, low_memory=True):
    """Returns a dataframe read from url. If do_parse_dates it returns a
    date-parsed dataframe. Low-memory option is to be passed to pandas.

    Args:
        url (str): source url
        do_parse_dates (bool, optional): Wether to parse a date column or not.
        Defaults to False.
        low_memory (bool, optional): use pandas' low_memory. Defaults to True.

    Returns:
        pd.DataFrame: the dataframe extracted from the provided url
    """
    logger.info(f"Extracting data at {url}")
    df = pd.DataFrame()
    if do_parse_dates:
        df = pd.read_csv(url, parse_dates=[VAX_DATE_KEY], low_memory=low_memory)
    else:
        df = pd.read_csv(url)
    df["extracted_at"] = dt.datetime.now()
    logger.info(f"Read {len(df.index)} records")
    return df
