from airflow.decorators import dag, task
from etl.od.collections import pop_coll, vax_admins_coll, vax_admins_summary_coll
from etl.od.extract import get_od_df
from etl.od.load import load_preprocessed_od_df_to_mongo
from etl.od.preprocess import preprocess_vax_admins_df, preprocess_vax_admins_summary_df
from etl.od.settings import DAG_ID, SCHEDULE_INTERVAL
from settings.constants import DEFAULT_DAG_ARGS
from settings.urls import (
    URL_VAX_ADMINS_DATA,
    URL_VAX_ADMINS_SUMMARY_DATA,
    URL_VAX_POP_DATA,
)
from utils.common import get_logger

logger = get_logger("OD-DAG")


@dag(
    DAG_ID,
    catchup=False,
    tags=["italia", "covid", "vaccines", "opendata"],
    default_args=DEFAULT_DAG_ARGS,
    schedule_interval=SCHEDULE_INTERVAL,
)
def od_etl():
    """
    Italy Open-Data vaccination ETL procedure.
    This DAG extracts the vaccine data from Italia Open Data
    [repository](https://github.com/italia/covid19-opendata-vaccini/).

    It creates the following collections:
      - Vax (daily vax update per area and provider)
      - VaxSummary (latest summary)
      - Population (per-region population breakdown)
    """

    @task()
    def extract_vax_data():
        logger.info(f"Extracting vax data at {URL_VAX_ADMINS_DATA}")
        df = get_od_df(url=URL_VAX_ADMINS_DATA, do_parse_dates=True, low_memory=False)
        return df

    @task()
    def transform_vax_data(vax_admins_df):
        logger.info("Transforming vax OD data")
        df = preprocess_vax_admins_df(vax_admins_df)
        logger.info(f"Transformed {len(df.index)} records")
        return df

    @task()
    def load_vax_data(preprocessed_vax_df):
        load_preprocessed_od_df_to_mongo(
            df=preprocessed_vax_df,
            collection=vax_admins_coll,
            data_type="OD vax data",
            ordered=True,
        )

    @task()
    def extract_vax_summary_data():
        logger.info(f"Extracting vax OD summary at {URL_VAX_ADMINS_SUMMARY_DATA}")
        df = get_od_df(url=URL_VAX_ADMINS_SUMMARY_DATA, do_parse_dates=True)
        logger.info(f"Extracted {len(df.index)} records")
        return df

    @task()
    def transform_vax_summary_data(vax_admins_summary_df):
        logger.info("Transforming vax summary data")
        df = preprocess_vax_admins_summary_df(vax_admins_summary_df)
        logger.info(f"Transformed {len(df.index)} records")
        return df

    @task()
    def load_vax_summary_data(preprocessed_vax_admins_summary):
        load_preprocessed_od_df_to_mongo(
            df=preprocessed_vax_admins_summary,
            collection=vax_admins_summary_coll,
            data_type="OD vax summary data",
            ordered=True,
        )

    @task()
    def copy_pop_data():
        df = get_od_df(url=URL_VAX_POP_DATA)
        load_preprocessed_od_df_to_mongo(
            df=df, collection=pop_coll, data_type="OD Population data"
        )

    copy_pop_data()

    vax_data = extract_vax_data()
    preprocessed_vax_data = transform_vax_data(vax_data)
    load_vax_data(preprocessed_vax_data)

    vax_summary_data = extract_vax_summary_data()
    preprocessed_vax_summary_data = transform_vax_summary_data(vax_summary_data)
    load_vax_summary_data(preprocessed_vax_summary_data)


VAX_ETL = od_etl()
