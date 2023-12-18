import pandas as pd
from airflow.decorators import dag, task
from etl.od.collections import pop_coll, vax_admins_coll, vax_admins_summary_coll
from etl.od.preprocess import preprocess_vax_admins_df, preprocess_vax_admins_summary_df
from settings.constants import DEFAULT_DAG_ARGS
from settings.urls import (
    URL_VAX_ADMINS_DATA,
    URL_VAX_ADMINS_SUMMARY_DATA,
    URL_VAX_POP_DATA,
)
from settings.vars import VAX_DATE_KEY
from utils.misc import get_logger

logger = get_logger("OD-ETL")


@dag("OpenData_ETL", catchup=False, tags=["COVID"], default_args=DEFAULT_DAG_ARGS)
def od_etl():
    """
    Italy Open-Data vaccination ETL procedure
    """

    @task()
    def vax_administrations_etl():
        """Create vaccine administrations colleciton"""
        logger.info(f"Reading {URL_VAX_ADMINS_DATA}")
        vax_admins_df = pd.read_csv(
            URL_VAX_ADMINS_DATA, parse_dates=[VAX_DATE_KEY], low_memory=False
        )
        preprocessed_vax_admins_df = preprocess_vax_admins_df(vax_admins_df)
        records = preprocessed_vax_admins_df.to_dict(orient="records")
        logger.info("Creating vax admins collection")
        vax_admins_coll.drop()
        vax_admins_coll.insert_many(records, ordered=True)

    @task()
    def vax_administrations_summary_etl():
        """Create vaccine administrations summary colleciton"""
        logger.info(f"Reading {URL_VAX_ADMINS_SUMMARY_DATA}")
        vax_admins_summary_df = pd.read_csv(
            URL_VAX_ADMINS_SUMMARY_DATA, parse_dates=[VAX_DATE_KEY]
        )
        preprocessed_vax_admins_summary = preprocess_vax_admins_summary_df(
            vax_admins_summary_df
        )
        records = preprocessed_vax_admins_summary.to_dict(orient="records")
        logger.info("Creating vax admins summary collection")
        vax_admins_summary_coll.drop()
        vax_admins_summary_coll.insert_many(records, ordered=True)

    @task()
    def population_data_etl():
        """Create OD population collection"""
        pop_df = pd.read_csv(URL_VAX_POP_DATA)
        records = pop_df.to_dict(orient="records")
        logger.info("Creating population collection")
        pop_coll.drop()
        pop_coll.insert_many(records)

    vax_administrations_etl()
    vax_administrations_summary_etl()
    population_data_etl()


VAX_ETL = od_etl()
