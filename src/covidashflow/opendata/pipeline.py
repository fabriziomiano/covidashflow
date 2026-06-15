"""Extract and load orchestration for Italia Open Data vaccination data."""

from dataclasses import dataclass

import pandas as pd

from covidashflow.common.dataframes import read_csv_frame
from covidashflow.common.logging import get_logger
from covidashflow.common.mongo import MongoCollections, replace_collection
from covidashflow.common.urls import (
    URL_VAX_ADMINS_DATA,
    URL_VAX_ADMINS_SUMMARY_DATA,
    URL_VAX_POP_DATA,
)
from covidashflow.common.vars import VAX_DATE_KEY
from covidashflow.opendata.transform import (
    get_region_pop_dict,
    preprocess_vax_admins_df,
    preprocess_vax_admins_summary_df,
)

logger = get_logger(__name__)


@dataclass(frozen=True)
class OpenDataSources:
    """Source URLs for all Italia Open Data vaccination datasets."""

    vax_admins_url: str = URL_VAX_ADMINS_DATA
    vax_admins_summary_url: str = URL_VAX_ADMINS_SUMMARY_DATA
    population_url: str = URL_VAX_POP_DATA


def extract_opendata_frame(
    url: str,
    *,
    parse_dates: bool = False,
    low_memory: bool = True,
) -> pd.DataFrame:
    """Read an Italia Open Data CSV source into a timestamped dataframe."""
    logger.info("Extracting Open Data at %s", url)
    df = read_csv_frame(
        url,
        date_column=VAX_DATE_KEY if parse_dates else None,
        low_memory=low_memory,
    )
    logger.info("Read %s Open Data records", len(df.index))
    return df


def run_opendata_pipeline(
    collections: MongoCollections,
    sources: OpenDataSources = OpenDataSources(),
) -> None:
    """Run the complete vaccination ETL pipeline against Mongo collections."""
    population_df = extract_opendata_frame(sources.population_url)
    replace_collection(
        collections.population,
        population_df.to_dict(orient="records"),
    )

    vax_df = extract_opendata_frame(
        sources.vax_admins_url,
        parse_dates=True,
        low_memory=False,
    )
    preprocessed_vax_df = preprocess_vax_admins_df(vax_df)
    replace_collection(
        collections.vax_admins,
        preprocessed_vax_df.to_dict(orient="records"),
        ordered=True,
    )

    summary_df = extract_opendata_frame(sources.vax_admins_summary_url, parse_dates=True)
    population = get_region_pop_dict(collections.population)
    preprocessed_summary_df = preprocess_vax_admins_summary_df(summary_df, population)
    replace_collection(
        collections.vax_admins_summary,
        preprocessed_summary_df.to_dict(orient="records"),
        ordered=True,
    )
