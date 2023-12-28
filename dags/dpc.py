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
from etl.dpc.extract import get_dpc_df
from etl.dpc.load import load_dpc_records_to_mongo
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
from etl.dpc.settings import DAG_ID, SCHEDULE_INTERVAL
from settings.constants import DEFAULT_DAG_ARGS
from settings.urls import URL_NATIONAL, URL_PROVINCIAL, URL_REGIONAL
from utils.common import get_logger

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
    PCM-DPC Pandemic data ETL procedure.
    This DAG extracts the pandemic data from the Civil Protection Dept.
    [repository](https://github.com/pcm-dpc/COVID-19/).
    It creates the following collections:
      - National (raw, trends, series)
      - Regional (raw, trends, series, breakdown)
      - Provincial (raw, trends, series, breakdown)
    """

    @task()
    def extract_national_data():
        df = get_dpc_df(url=URL_NATIONAL, do_parse_dates=True)
        return df

    @task()
    def transform_national_data(national_data_df):
        logger.info("Transforming national data")
        df = preprocess_national_df(national_data_df)
        logger.info(f"Transformed {len(df.index)} records")
        return df

    @task()
    def load_national_data(preprocessed_national_data_df):
        national_data = preprocessed_national_data_df.to_dict(orient="records")
        load_dpc_records_to_mongo(
            records=national_data,
            collection=nat_data_coll,
            data_type="DPC national data",
            ordered=True,
        )

    @task()
    def load_national_trends(preprocessed_national_data_df):
        national_trends = build_national_trends(preprocessed_national_data_df)
        load_dpc_records_to_mongo(
            records=national_trends,
            collection=nat_trends_coll,
            data_type="DPC national trends",
        )

    @task()
    def load_national_series(preprocessed_national_data_df):
        logger.info(f"Loading national series to {nat_series_coll}")
        national_series = build_national_series(preprocessed_national_data_df)
        nat_series_coll.drop()
        result_one = nat_series_coll.insert_one(national_series)
        logger.info(f"Inserted record with id {result_one.inserted_id}")

    @task()
    def extract_regional_data():
        """
        Get DPC Regional data
        """
        df = get_dpc_df(url=URL_REGIONAL, do_parse_dates=True)
        return df

    @task()
    def transform_regional_data(regional_data_df):
        logger.info("Transforming regional DPC data")
        df = preprocess_regional_df(regional_data_df)
        logger.info(f"Transformed {len(df.index)} records")
        return df

    @task()
    def load_regional_data(preprocessed_regional_data_df):
        regional_records = preprocessed_regional_data_df.to_dict(orient="records")
        load_dpc_records_to_mongo(
            records=regional_records,
            collection=reg_data_coll,
            data_type="DPC regional data",
        )

    @task()
    def load_regional_breakdown(preprocessed_regional_data_df):
        logger.info(f"Loading DPC regional breakdown to {reg_breakdown_coll}")
        regional_breakdown = build_regional_breakdown(preprocessed_regional_data_df)
        reg_breakdown_coll.drop()
        result_one = reg_breakdown_coll.insert_one(regional_breakdown)
        logger.info(f"Inserted record with id {result_one.inserted_id}")

    @task()
    def load_regional_series(preprocessed_regional_data_df):
        regional_series = build_regional_series(preprocessed_regional_data_df)
        load_dpc_records_to_mongo(
            records=regional_series,
            collection=reg_series_coll,
            data_type="DPC regional series",
        )

    @task()
    def load_regional_trends(preprocessed_regional_data_df):
        regional_trends = build_regional_trends(preprocessed_regional_data_df)
        load_dpc_records_to_mongo(
            records=regional_trends,
            collection=reg_trends_coll,
            data_type="DPC regional trends",
        )

    @task()
    def extract_provincial_data():
        df = get_dpc_df(url=URL_PROVINCIAL, do_parse_dates=True)
        return df

    @task()
    def transform_provincial_data(provincial_data_df):
        logger.info("Transforming provincial data")
        df = preprocess_provincial_df(provincial_data_df)
        logger.info(f"Transformed {len(df.index)} records")
        return df

    @task()
    def load_provincial_data(preprocessed_provincial_data_df):
        provincial_records = preprocessed_provincial_data_df.to_dict(orient="records")
        load_dpc_records_to_mongo(
            records=provincial_records,
            collection=prov_data_coll,
            data_type="DPC Provincial data",
            ordered=True,
        )

    @task
    def load_provincial_breakdown(preprocessed_provincial_data_df):
        breakdown = build_provincial_breakdowns(preprocessed_provincial_data_df)
        load_dpc_records_to_mongo(
            records=breakdown,
            collection=prov_breakdown_coll,
            data_type="DPC Provincial breakdown",
        )

    @task()
    def load_provincial_series(preprocessed_provincial_data_df):
        provincial_series = build_provincial_series(preprocessed_provincial_data_df)
        load_dpc_records_to_mongo(
            records=provincial_series,
            collection=prov_series_coll,
            data_type="DPC Provincial series",
        )

    @task()
    def load_provincial_trends(preprocessed_provincial_data_df):
        provincial_trends = build_provincial_trends(preprocessed_provincial_data_df)
        load_dpc_records_to_mongo(
            records=provincial_trends,
            collection=prov_trends_coll,
            data_type="DPC Provincial trends",
        )

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
