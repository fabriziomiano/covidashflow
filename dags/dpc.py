from airflow.decorators import dag, task
from etl.dpc.collections import (
    nat_data_coll,
    nat_series_coll,
    nat_trends_coll,
    prov_breakdown_coll,
    prov_data_coll,
    prov_series_coll,
    prov_trends_coll,
    reg_breakdown_coll,
    reg_data_coll,
    reg_series_coll,
    reg_trends_coll,
)
from etl.dpc.extract import load_dpc_df
from etl.dpc.preprocess import (
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
from settings.constants import DEFAULT_DAG_ARGS
from settings.urls import URL_NATIONAL, URL_PROVINCIAL, URL_REGIONAL
from utils.misc import get_logger

from dags.etl.dpc.settings import DAG_ID, SCHEDULE_INTERVAL

logger = get_logger("DPC-DAG")


@dag(
    DAG_ID,
    catchup=False,
    tags=["italia", "dpc-pcm", "covid"],
    default_args=DEFAULT_DAG_ARGS,
    schedule_interval=SCHEDULE_INTERVAL,
)
def pcm_dpc_etl():
    """
    DPC-PCM ETL procedure
    """

    """
    National DPC Data
    """

    @task()
    def extract_national_data():
        logger.info(f"Extracting national data at {URL_NATIONAL}")
        df = load_dpc_df(url=URL_NATIONAL, do_parse_dates=True)
        logger.info(f"Extracted {len(df.index)} records")
        return df

    @task()
    def transform_national_data(national_data_df):
        logger.info("Transforming national data")
        df = preprocess_national_df(national_data_df)
        logger.info(f"Transformed {len(df.index)} records")
        return df

    @task()
    def load_national_data(preprocessed_national_data_df):
        logger.info(f"Loading national data to {nat_data_coll}")
        records = preprocessed_national_data_df.to_dict(orient="records")
        nat_data_coll.drop()
        result_many = nat_data_coll.insert_many(records, ordered=True)
        logger.info(f"Landed {len(result_many.inserted_ids)} records")

    @task()
    def load_national_trends(preprocessed_national_data_df):
        logger.info(f"Loading national trends data to {nat_trends_coll}")
        national_trends = build_national_trends(preprocessed_national_data_df)
        nat_trends_coll.drop()
        result_many = nat_trends_coll.insert_many(national_trends)
        logger.info(f"Landed {len(result_many.inserted_ids)} records")

    @task()
    def load_national_series(preprocessed_national_data_df):
        logger.info(f"Loading national series data to {nat_series_coll}")
        national_series = build_national_series(preprocessed_national_data_df)
        nat_series_coll.drop()
        result_one = nat_series_coll.insert_one(national_series)
        logger.info(f"Inserted record with id {result_one.inserted_id}")

    """
    Regional DPC Data
    """

    @task()
    def extract_regional_data():
        """
        Get DPC Regional data
        """
        logger.info(f"Extracting regional DPC Data at {URL_REGIONAL}")
        df = load_dpc_df(url=URL_REGIONAL, do_parse_dates=True)
        logger.info(f"Extracted {len(df.index)} records")
        return df

    @task()
    def transform_regional_data(regional_data_df):
        logger.info("Transforming regional DPC data")
        df = preprocess_regional_df(regional_data_df)
        logger.info(f"Transformed {len(df.index)} records")
        return df

    @task()
    def load_regional_data(preprocessed_regional_data_df):
        logger.info("Loading regional data")
        regional_records = preprocessed_regional_data_df.to_dict(orient="records")
        reg_data_coll.drop()
        result_many = reg_data_coll.insert_many(regional_records, ordered=True)
        logger.info(f"Landed {len(result_many.inserted_ids)} records")

    @task()
    def load_regional_breakdown(preprocessed_regional_data_df):
        regional_breakdown = build_regional_breakdown(preprocessed_regional_data_df)
        reg_breakdown_coll.drop()
        result_one = reg_breakdown_coll.insert_one(regional_breakdown)
        logger.info(f"Inserted record with id {result_one.inserted_id}")

    @task()
    def load_regional_series(preprocessed_regional_data_df):
        regional_series = build_regional_series(preprocessed_regional_data_df)
        reg_series_coll.drop()
        result_many = reg_series_coll.insert_many(regional_series)
        logger.info(f"Landed {len(result_many.inserted_ids)} records")

    @task()
    def load_regional_trends(preprocessed_regional_data_df):
        regional_trends = build_regional_trends(preprocessed_regional_data_df)
        reg_trends_coll.drop()
        result_many = reg_trends_coll.insert_many(regional_trends)
        logger.info(f"Landed {len(result_many.inserted_ids)} records")

    """
    Provincial DPC data
    """

    @task()
    def extract_provincial_data():
        logger.info(f"Extracting provincial DPC data at {URL_PROVINCIAL}")
        df = load_dpc_df(url=URL_PROVINCIAL, do_parse_dates=True)
        logger.info(f"Extracted {len(df.index)} records")
        return df

    @task()
    def transform_provincial_data(provincial_data_df):
        logger.info("Transforming provincial data")
        df = preprocess_provincial_df(provincial_data_df)
        logger.info(f"Transformed {len(df.index)} records")
        return df

    @task()
    def load_provincial_data(preprocessed_provincial_data_df):
        logger.info(f"Loading provincial data to {prov_data_coll}")
        provincial_records = preprocessed_provincial_data_df.to_dict(orient="records")
        prov_data_coll.drop()
        result_many = prov_data_coll.insert_many(provincial_records, ordered=True)
        logger.info(f"Landed {len(result_many.inserted_ids)} records")

    @task
    def load_provincial_breakdown(preprocessed_provincial_data_df):
        logger.info(f"Loading provincial breakdown to {prov_breakdown_coll}")
        provincial_breakdowns = build_provincial_breakdowns(
            preprocessed_provincial_data_df
        )
        prov_breakdown_coll.drop()
        result_many = prov_breakdown_coll.insert_many(provincial_breakdowns)
        logger.info(f"Landed {len(result_many.inserted_ids)} records")

    @task()
    def load_provincial_series(preprocessed_provincial_data_df):
        logger.info(f"Loading provincial series to {prov_series_coll}")
        provincial_series = build_provincial_series(preprocessed_provincial_data_df)
        prov_series_coll.drop()
        result_many = prov_series_coll.insert_many(provincial_series)
        logger.info(f"Landed {len(result_many.inserted_ids)} records")

    @task()
    def load_provincial_trends(preprocessed_provincial_data_df):
        logger.info(f"Loading provincial trends to {prov_trends_coll}")
        provincial_trends = build_provincial_trends(preprocessed_provincial_data_df)
        prov_trends_coll.drop()
        result_many = prov_trends_coll.insert_many(provincial_trends)
        logger.info(f"Landed {len(result_many.inserted_ids)} records")

    national_data = extract_national_data()
    preprocessed_national_data = transform_national_data(national_data)
    load_national_data(preprocessed_national_data)
    load_national_series(preprocessed_national_data)
    load_national_trends(preprocessed_national_data)

    regional_data = extract_regional_data()
    preprocessed_regional_data = transform_regional_data(regional_data)
    load_regional_data(preprocessed_regional_data)
    load_regional_series(preprocessed_regional_data)
    load_regional_trends(preprocessed_regional_data)
    load_regional_breakdown(preprocessed_regional_data)

    provincial_data = extract_provincial_data()
    preprocessed_provincial_data = transform_provincial_data(provincial_data)
    load_provincial_data(preprocessed_provincial_data)
    load_provincial_series(preprocessed_provincial_data)
    load_provincial_trends(preprocessed_provincial_data)
    load_provincial_breakdown(preprocessed_provincial_data)


DPC_ETL = pcm_dpc_etl()
