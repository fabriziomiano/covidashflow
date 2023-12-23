import pandas as pd
from airflow.decorators import dag, task
from etl.od.collections import pop_coll, vax_admins_coll, vax_admins_summary_coll
from etl.od.preprocess import preprocess_vax_admins_df, preprocess_vax_admins_summary_df
from etl.od.settings import DAG_ID, SCHEDULE_INTERVAL
from settings.constants import DEFAULT_DAG_ARGS
from settings.urls import (
    URL_VAX_ADMINS_DATA,
    URL_VAX_ADMINS_SUMMARY_DATA,
    URL_VAX_POP_DATA,
)
from settings.vars import VAX_DATE_KEY
from utils.misc import get_logger

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
    Italy Open-Data vaccination ETL procedure
    """

    @task()
    def extract_vax_data():
        logger.info(f"Extracting vax data at {URL_VAX_ADMINS_DATA}")
        df = pd.read_csv(
            URL_VAX_ADMINS_DATA, parse_dates=[VAX_DATE_KEY], low_memory=False
        )
        logger.info(f"Extracted {len(df.index)} records")
        return df

    @task()
    def transform_vax_data(vax_admins_df):
        logger.info("Transforming vax OD data")
        df = preprocess_vax_admins_df(vax_admins_df)
        logger.info(f"Transformed {len(df.index)} records")
        return df

    @task()
    def load_vax_data(preprocessed_vax_df):
        logger.info(f"Loading vax OD data to {vax_admins_coll}")
        records = preprocessed_vax_df.to_dict(orient="records")
        vax_admins_coll.drop()
        result_many = vax_admins_coll.insert_many(records, ordered=True)
        logger.info(f"Landed {len(result_many.inserted_ids)} records")

    @task()
    def extract_vax_summary_data():
        logger.info(f"Extracting vax OD summary at {URL_VAX_ADMINS_SUMMARY_DATA}")
        df = pd.read_csv(URL_VAX_ADMINS_SUMMARY_DATA, parse_dates=[VAX_DATE_KEY])
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
        logger.info(f"Loading vax OD data to {vax_admins_summary_coll}")
        records = preprocessed_vax_admins_summary.to_dict(orient="records")
        logger.info("Creating vax admins summary collection")
        vax_admins_summary_coll.drop()
        result_many = vax_admins_summary_coll.insert_many(records, ordered=True)
        logger.info(f"Landed {len(result_many.inserted_ids)} records")

    @task()
    def copy_pop_data():
        logger.info(f"Extracting population data at {URL_VAX_POP_DATA}")
        df = pd.read_csv(URL_VAX_POP_DATA)
        logger.info(f"Read {len(df.index)} records")
        records = df.to_dict(orient="records")
        logger.info("Creating population collection")
        pop_coll.drop()
        result_many = pop_coll.insert_many(records)
        logger.info(f"Inserted {result_many.inserted_ids} records")

    copy_pop_data()

    vax_data = extract_vax_data()
    preprocessed_vax_data = transform_vax_data(vax_data)
    load_vax_data(preprocessed_vax_data)

    vax_summary_data = extract_vax_summary_data()
    preprocessed_vax_summary_data = transform_vax_summary_data(vax_summary_data)
    load_vax_summary_data(preprocessed_vax_summary_data)


VAX_ETL = od_etl()
