"""
OD Preprocessing module
"""
import pandas as pd
from etl.od.collections import pop_coll
from settings.constants import OD_TO_PC_MAP
from settings.vars import (
    F_SEX_KEY,
    M_SEX_KEY,
    OD_NUTS1_KEY,
    OD_NUTS2_KEY,
    OD_POP_KEY,
    OD_REGION_CODE,
    POP_KEY,
    VAX_AGE_KEY,
    VAX_AREA_KEY,
    VAX_BOOSTER1_DOSE_KEY,
    VAX_BOOSTER2_DOSE_KEY,
    VAX_BOOSTER3_DOSE_KEY,
    VAX_DATE_FMT,
    VAX_DATE_KEY,
    VAX_FIRST_DOSE_KEY,
    VAX_PROVIDER_KEY,
    VAX_SECOND_DOSE_KEY,
    VAX_TOT_ADMINS_KEY,
)
from utils.common import get_logger

logger = get_logger(__name__)


def get_region_pop_dict():
    """
    Return a region:population dict
    :return: dict
    """
    it_pop_dict = {}
    try:
        pop_pipe = [
            {
                "$group": {
                    "_id": {VAX_AREA_KEY: f"${VAX_AREA_KEY}"},
                    f"{OD_POP_KEY}": {"$sum": f"${OD_POP_KEY}"},
                },
            }
        ]
        records = list(pop_coll.aggregate(pipeline=pop_pipe))
        it_pop_dict = {
            OD_TO_PC_MAP[r["_id"][VAX_AREA_KEY]]: r[OD_POP_KEY] for r in records
        }
    except Exception as e:
        logger.error(f"While getting region pop dict: {e}")
    return it_pop_dict


def preprocess_vax_admins_df(df):
    """
    Return a modified version of the input df.
    Add two columns '_id' and 'totale'.
    The former is computed as the concatenation of three strings
    VAX_DATE_KEY + VAX_AGE_KEY + VAX_PROVIDER_KEY, and the latter as the sum of
    the columns M_SEX_KEY + F_SEX_KEY
    :param df: pandas.DataFrame
    :return: pandas.DataFrame
    """
    df[VAX_AGE_KEY] = (
        df[VAX_AGE_KEY]
        .apply(lambda x: x.strip())
        .apply(lambda x: x.replace("80-89", "80+"))
        .apply(lambda x: x.replace("90+", "80+"))
    )
    df = (
        df.groupby(
            by=[
                VAX_DATE_KEY,
                VAX_PROVIDER_KEY,
                VAX_AREA_KEY,
                VAX_AGE_KEY,
                OD_NUTS1_KEY,
                OD_NUTS2_KEY,
                OD_REGION_CODE,
            ]
        )
        .sum(
            [
                F_SEX_KEY,
                M_SEX_KEY,
                VAX_FIRST_DOSE_KEY,
                VAX_SECOND_DOSE_KEY,
                VAX_BOOSTER1_DOSE_KEY,
                VAX_BOOSTER2_DOSE_KEY,
                VAX_BOOSTER3_DOSE_KEY,
                VAX_TOT_ADMINS_KEY,
            ]
        )
        .reset_index()
    )
    df["totale"] = df[M_SEX_KEY] + df[F_SEX_KEY]
    df["_id"] = (
        df[VAX_DATE_KEY].apply(lambda x: x.strftime(VAX_DATE_FMT))
        + df[VAX_AREA_KEY]
        + df[VAX_AGE_KEY]
        + df[VAX_PROVIDER_KEY]
    )
    return df


def preprocess_vax_admins_summary_df(df):
    """
    Returns a preprocessed version of the input df.
    Resamples the df by filling in the missing dates per area.
    Adds two columns '_id' and POP_TOT_KEY.
    The former is computed as the concatenation of two strings
    VAX_DATE_KEY + VAX_AREA_KEY, and the latter is taken from the
    ITALY_POPULATION dict.

    :param df: pandas.DataFrame
    :return: pandas.DataFrame
    """
    population = get_region_pop_dict()
    print(f"Pop dict {population}")
    print(f"od to pc map {OD_TO_PC_MAP}")
    out_df = pd.DataFrame()
    for r in df[VAX_AREA_KEY].unique():
        # slice per area
        reg_df = df[df[VAX_AREA_KEY] == r]

        # add missing dates by resampling VAX_DATE_KEY col per day
        reg_df = reg_df.set_index(VAX_DATE_KEY).resample("1D").asfreq()

        # Fill NaN in each column: if str with fast fill else 0
        for col in reg_df.columns:
            if isinstance(reg_df[col].to_numpy()[-1], str):
                reg_df[col].ffill(inplace=True)
            else:
                reg_df[col].fillna(0.0, inplace=True)

        out_df = pd.concat([out_df, reg_df])  # ignore_index=True)
    out_df.reset_index(inplace=True)
    out_df["_id"] = (
        out_df[VAX_DATE_KEY].apply(lambda x: x.strftime(VAX_DATE_FMT))
        + out_df[VAX_AREA_KEY]
    )
    out_df[POP_KEY] = out_df[VAX_AREA_KEY].apply(lambda x: population[OD_TO_PC_MAP[x]])
    return out_df
