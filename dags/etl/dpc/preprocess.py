"""
DPC Preprocessing module
"""
import pandas as pd
from settings.constants import PROVINCES, REGIONS
from settings.vars import (
    DAILY_QUANTITIES,
    DAILY_SWABS_KEY,
    DATE_KEY,
    NEW_POSITIVE_KEY,
    NEW_POSITIVE_MA_KEY,
    NON_CUM_QUANTITIES,
    POSITIVITY_INDEX,
    PROV_TREND_CARDS,
    PROVINCE_CODE,
    PROVINCE_KEY,
    REGION_CODE,
    REGION_KEY,
    STATE_KEY,
    TOTAL_CASES_KEY,
    TREND_CARDS,
    VARS,
)
from utils.common import get_logger

pd.options.mode.chained_assignment = None
COLUMNS_TO_DROP = [STATE_KEY]


logger = get_logger(__name__)


def add_delta(df):
    """
    Add a difference column "*_g" for all the daily-type variables in VARS
    that also exist in df
    :param df: pd.DataFrame
    :return: pd.DataFrame
    """
    columns = [col for col in VARS if VARS[col]["type"] != "daily"]
    for col in columns:
        try:
            df[col + "_g"] = df[col].diff()
        except KeyError:
            continue
    return df


def add_percentages(df):
    """
    Add a percentage column "*_perc" for all the variables in VARS that exists
    in df
    :param df: pd.DataFrame
    :return: pd.DataFrame
    """
    for col in VARS:
        try:
            diff_df = df[col].diff(periods=7)
            df[col + "_perc"] = diff_df.div(df[col].shift(7).abs()) * 100
        except KeyError:
            continue
    return df


def add_positivity_idx(df):
    """
    Add a DAILY_POSITIVITY_INDEX as NEW_POSITIVE_KEY / DAILY_SWABS_KEY * 100
    :param df: pd.DataFrame
    :return: pd.DataFrame
    """
    df[POSITIVITY_INDEX] = (df[NEW_POSITIVE_KEY].div(df[DAILY_SWABS_KEY]) * 100).round(
        decimals=2
    )
    return df


def add_moving_avg(df):
    """Add weekly moving average to the daily quantities"""
    cols = [
        col for col in VARS if VARS[col]["type"] == "daily" and not col.endswith("_ma")
    ]
    for col in cols:
        try:
            df[col + "_ma"] = df[col].rolling(7).mean()
            df[col + "_ma"] = clean_df(df[col + "_ma"])
            df[col + "_ma"] = df[col + "_ma"].astype(int)
        except KeyError:
            continue
    return df


def clean_df(df):
    """
    Replace all nan values with None and then with 0
    :param df: pd.DataFrame
    :return: pd.DataFrame
    """
    df = df.where(pd.notnull(df), None)
    df = df.fillna(value=0)
    return df


def preprocess_national_df(df):
    """
    Preprocess the national PC DataFrame:
     add the delta, the percentages, and the positivity index
    :param df: pd.DataFrame
    :return: pd.DataFrame
    """
    df = add_delta(df)
    df = add_moving_avg(df)
    df = add_percentages(df)
    df = add_positivity_idx(df)
    df = clean_df(df)
    return df


def preprocess_regional_df(df):
    """
    Preprocess the regional PC DataFrame:
     add the delta, the percentages, and the positivity index to every
     region sub-df
    :param df: pd.DataFrame
    :return: pd.DataFrame
    """
    dfs = []
    for rc in df[REGION_CODE].unique():
        df_region = df[df[REGION_CODE] == rc].copy()
        df_region = add_delta(df_region)
        df_region = add_moving_avg(df_region)
        df_region = add_percentages(df_region)
        df_region = add_positivity_idx(df_region)
        dfs.append(df_region)
    out_df = pd.concat(dfs)
    out_df = clean_df(out_df)
    return out_df


def preprocess_provincial_df(df):
    """
    Preprocess the provincial PC DataFrame:
     add the new positive and the relevant percentage
    :param df: pd.DataFrame
    :return: pd.DataFrame
    """
    df = df.rename(columns=lambda x: x.strip())
    dfs = []
    for pc in df[PROVINCE_CODE].unique():
        dfp = df[df[PROVINCE_CODE] == pc].copy()
        dfp[NEW_POSITIVE_KEY] = dfp[TOTAL_CASES_KEY].diff()
        df_new_pos_diff = dfp[NEW_POSITIVE_KEY].diff(periods=7)
        dfp["nuovi_positivi_perc"] = (
            df_new_pos_diff.div(dfp[NEW_POSITIVE_KEY].shift(7).abs()) * 100
        )
        dfp["totale_casi_perc"] = (
            dfp[NEW_POSITIVE_KEY].div(dfp[TOTAL_CASES_KEY].shift(7).abs()) * 100
        )
        dfp = add_moving_avg(dfp)
        dfs.append(dfp)
    out_df = pd.concat(dfs)
    out_df = clean_df(out_df)
    return out_df


def build_trend(df, col):
    """
    Return a trend object for a given df and variable col
    :param df: pd.DataFrame
    :param col: str
    :return: dict
    """
    status = "stable"
    perc_col = col + "_perc"
    df[col] = df[col].astype("int")
    count = df[col].to_numpy()[-1].item()
    last_week_count = df[col].to_numpy()[-8].item()
    try:
        df[perc_col].dropna(inplace=True)
        percentage = f"{round(df[perc_col].to_numpy()[-1])}%"
    except (OverflowError, TypeError):
        percentage = "n/a"
    if count < last_week_count:
        status = "decrease"
    if count > last_week_count:
        status = "increase"
    if count == last_week_count:
        status = "stable"
    trend = {
        "id": col,
        "type": VARS[col]["type"],
        "title": VARS[col]["title"],
        "desc": VARS[col]["desc"],
        "longdesc": VARS[col]["longdesc"],
        "count": count,
        "colour": VARS[col][status]["colour"],
        "icon": VARS[col]["icon"],
        "status_icon": VARS[col][status]["icon"],
        "tooltip": VARS[col][status]["tooltip"],
        "percentage_difference": percentage,
        "last_week_count": last_week_count,
        "last_week_dt": df[DATE_KEY].iloc[-8],
    }
    return trend


def build_national_trends(df):
    """
    Return the list of national-trend variables for a given national df
    :param df: pd.DataFrame
    :return: list
    """
    trends = []
    for col in TREND_CARDS:
        try:
            t = build_trend(df, col)
            trends.append(t)
        except Exception as e:
            logger.error(f"{e}")
            continue
    return trends


def build_regional_trends(df):
    """
    Return the list of regional-trend variables for a given regional df
    :param df: pd.DataFrame
    :return: list
    """
    trends = []
    for cr in df[REGION_CODE].unique():
        df_r = df[df[REGION_CODE] == cr].copy()
        trend = {
            REGION_KEY: df_r[REGION_KEY].to_numpy()[-1],
            "trends": build_national_trends(df_r),
        }
        trends.append(trend)
    return trends


def build_provincial_trends(df):
    """
    Return the list of provincial-trend variables for a given provincial df
    :param df: pd.DataFrame
    :return: list
    """
    trends = []
    for cp in df[PROVINCE_CODE].unique():
        province_trends = []
        df_province = df[df[PROVINCE_CODE] == cp].copy()
        for col in PROV_TREND_CARDS:
            try:
                trend = build_trend(df_province, col)
                province_trends.append(trend)
            except KeyError as e:
                logger.error(f"Error in build_provincial_trends: {e}")
                continue
        trends.append(
            {
                PROVINCE_KEY: df_province[PROVINCE_KEY].to_numpy()[-1],
                "trends": province_trends,
            }
        )
    return trends


def build_regional_breakdown(df):
    """
    Return a regional breakdown object for a given regional df
    :param df: pd.DataFrame
    :return: dict
    """
    breakdown = {}
    sub_url = "regions"
    for col in TREND_CARDS:
        breakdown[col] = []
        for rc in df[REGION_CODE].unique():
            df_area = df[df[REGION_CODE] == rc].copy()
            area = df_area[REGION_KEY].to_numpy()[-1]
            if area not in REGIONS:
                continue
            count = int(df_area[col].to_numpy()[-1])
            url = "/{}/{}".format(sub_url, df_area[REGION_KEY].to_numpy()[-1])
            b = {"area": area, "count": count, "url": url}
            breakdown[col].append(b)
    return breakdown


def build_provincial_breakdowns(df):
    """
    Return a list of breakdown objects for all the provinces in
     a given provincial df
    :param df: pd.DataFrame
    :return: list
    """
    breakdowns = []
    for rc in df[REGION_CODE].unique():
        df_region = df[df[REGION_CODE] == rc].copy()
        region = df_region[REGION_KEY].to_numpy()[-1]
        if region not in REGIONS:
            continue
        breakdown = {REGION_KEY: region, "breakdowns": {}}
        for col in PROV_TREND_CARDS:
            breakdown["breakdowns"][col] = []
            for cp in df_region[PROVINCE_CODE].unique():
                df_province = df_region[df_region[PROVINCE_CODE] == cp].copy()
                province = df_province[PROVINCE_KEY].to_numpy()[-1]
                if province not in PROVINCES:
                    continue
                count = int(df_province[col].to_numpy()[-1])
                url = f"/provinces/{province}"
                prov_col_dict = {"area": province, "count": count, "url": url}
                breakdown["breakdowns"][col].append(prov_col_dict)
        breakdowns.append(breakdown)
    return breakdowns


def build_series(df):
    """
    Return a series tuple where:
    the first element is a list of dates,
    the second element is the series of the daily-type variables,
    the third element is the series of the current-type variables,
    the fourth element is the series of the cum-type variables.
    :param df: pd.DataFrame
    :return: tuple
    """
    dates = df[
        DATE_KEY
    ].to_list()  # apply(lambda x: x.strftime(CHART_DATE_FMT)).tolist()
    series_daily = sorted(
        [
            {"id": col, "name": VARS[col]["title"], "data": df[col].tolist()}
            for col in DAILY_QUANTITIES
        ],
        key=lambda x: max(x[DATE_KEY]),
        reverse=True,
    )
    series_current = sorted(
        [
            {"id": col, "name": VARS[col]["title"], "data": df[col].tolist()}
            for col in NON_CUM_QUANTITIES
        ],
        key=lambda x: max(x[DATE_KEY]),
        reverse=True,
    )
    series = (dates, series_daily, series_current)
    return series


def build_national_series(df):
    """
    Return a series object for a given national df
    :param df: pd.DataFrame
    :return: dict
    """
    data_series = build_series(df)
    series = {
        "dates": data_series[0],
        "daily": data_series[1],
        "current": data_series[2],
    }
    return series


def build_regional_series(df):
    """
    Return a list of series object for each region in a given regional df.
    :param df: pd.DataFrame
    :return: list
    """
    regional_series = []
    for cr in df[REGION_CODE].unique():
        df_area = df[df[REGION_CODE] == cr].copy()
        series = build_series(df_area)
        regional_series.append(
            {
                REGION_KEY: df_area[REGION_KEY].to_numpy()[-1],
                "dates": series[0],
                "daily": series[1],
                "current": series[2],
            }
        )
    return regional_series


def build_provincial_series(df):
    """
    Return a list of series object for each province in a given provincial df.
    :param df: pd.DataFrame
    :return: list
    """
    provincial_series = []
    for cp in df[PROVINCE_CODE].unique():
        df_area = df[df[PROVINCE_CODE] == cp].copy()
        dates = df_area[DATE_KEY].to_list()
        series_daily = [
            {
                "id": NEW_POSITIVE_MA_KEY,
                "name": VARS[NEW_POSITIVE_MA_KEY]["title"],
                "data": df_area[NEW_POSITIVE_MA_KEY].to_numpy().tolist(),
            }
        ]
        series_cum = [
            {
                "id": TOTAL_CASES_KEY,
                "name": VARS[TOTAL_CASES_KEY]["title"],
                "data": df_area[TOTAL_CASES_KEY].to_numpy().tolist(),
            }
        ]
        provincial_series.append(
            {
                PROVINCE_KEY: df_area[PROVINCE_KEY].to_numpy()[-1],
                "dates": dates,
                "daily": series_daily,
                "cum": series_cum,
            }
        )
    return provincial_series
