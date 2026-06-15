"""Extract and load orchestration for PCM-DPC COVID-19 data."""

from dataclasses import dataclass

import pandas as pd

from covidashflow.common.dataframes import read_csv_frame
from covidashflow.common.logging import get_logger
from covidashflow.common.mongo import MongoCollections, replace_collection, replace_one
from covidashflow.common.urls import URL_NATIONAL, URL_PROVINCIAL, URL_REGIONAL
from covidashflow.common.vars import DATE_KEY
from covidashflow.dpc.transform import (
    COLUMNS_TO_DROP,
    build_national_series,
    build_national_trends,
    build_provincial_breakdowns,
    build_provincial_series,
    build_provincial_trends,
    build_regional_breakdown,
    build_regional_series,
    build_regional_trends,
    preprocess_national_df,
    preprocess_provincial_df,
    preprocess_regional_df,
)

logger = get_logger(__name__)


@dataclass(frozen=True)
class DpcSources:
    """Source URLs for all PCM-DPC datasets consumed by the pipeline."""

    national_url: str = URL_NATIONAL
    regional_url: str = URL_REGIONAL
    provincial_url: str = URL_PROVINCIAL


def extract_dpc_frame(
    url: str,
    *,
    parse_dates: bool = True,
    drop_columns: bool = False,
) -> pd.DataFrame:
    """Read a PCM-DPC CSV source into a timestamped dataframe."""
    logger.info("Extracting DPC data at %s", url)
    df = read_csv_frame(
        url,
        date_column=DATE_KEY if parse_dates else None,
        low_memory=False,
        drop_columns=COLUMNS_TO_DROP if drop_columns else None,
    )
    logger.info("Read %s DPC records", len(df.index))
    return df


def load_national_outputs(df: pd.DataFrame, collections: MongoCollections) -> None:
    """Replace national raw, trend, and series Mongo outputs."""
    national_data = df.to_dict(orient="records")
    replace_collection(collections.national_data, national_data, ordered=True)
    replace_collection(collections.national_trends, build_national_trends(df))
    replace_one(collections.national_series, build_national_series(df))


def load_regional_outputs(df: pd.DataFrame, collections: MongoCollections) -> None:
    """Replace regional raw, trend, series, and breakdown Mongo outputs."""
    regional_data = df.to_dict(orient="records")
    replace_collection(collections.regional_data, regional_data)
    replace_collection(collections.regional_series, build_regional_series(df))
    replace_collection(collections.regional_trends, build_regional_trends(df))
    replace_one(collections.regional_breakdown, build_regional_breakdown(df))


def load_provincial_outputs(df: pd.DataFrame, collections: MongoCollections) -> None:
    """Replace provincial raw, trend, series, and breakdown Mongo outputs."""
    provincial_data = df.to_dict(orient="records")
    replace_collection(collections.provincial_data, provincial_data, ordered=True)
    replace_collection(collections.provincial_series, build_provincial_series(df))
    replace_collection(collections.provincial_trends, build_provincial_trends(df))
    replace_collection(collections.provincial_breakdown, build_provincial_breakdowns(df))


def run_dpc_pipeline(collections: MongoCollections, sources: DpcSources = DpcSources()) -> None:
    """Run the complete PCM-DPC ETL pipeline against Mongo collections."""
    national_df = preprocess_national_df(extract_dpc_frame(sources.national_url))
    load_national_outputs(national_df, collections)

    regional_df = preprocess_regional_df(extract_dpc_frame(sources.regional_url))
    load_regional_outputs(regional_df, collections)

    provincial_df = preprocess_provincial_df(extract_dpc_frame(sources.provincial_url))
    load_provincial_outputs(provincial_df, collections)
