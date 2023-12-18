import pandas as pd
from etl.dpc.preprocess import COLUMNS_TO_DROP
from settings.vars import DATE_KEY


def load_dpc_df(url, do_parse_dates=False, drop_columns=False):
    """
    Return a CP dataframe without the columns defined in COLUMNS_TO_DROP
    :param url: str: CP-repository data URL
    :return: pd.DataFrame
    """
    df = pd.DataFrame()
    if do_parse_dates:
        df = pd.read_csv(url, parse_dates=[DATE_KEY], low_memory=False)
    else:
        df = pd.read_csv(url, low_memory=False)
    if drop_columns:
        df.drop(columns=COLUMNS_TO_DROP, inplace=True)
    return df
