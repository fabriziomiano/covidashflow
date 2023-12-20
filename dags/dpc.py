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


@dag("DPCPCM_ETL", catchup=False, tags=["COVID"], default_args=DEFAULT_DAG_ARGS)
def pcm_dpc_etl():
    """
    DPC-PCM ETL procedure
    """

    national_data_df = load_dpc_df(url=URL_NATIONAL, do_parse_dates=True)
    preprocessed_national_data_df = preprocess_national_df(national_data_df)

    regional_data_df = load_dpc_df(url=URL_REGIONAL, do_parse_dates=True)
    preprocessed_regional_data_df = preprocess_regional_df(regional_data_df)

    provincial_data_df = load_dpc_df(url=URL_PROVINCIAL, do_parse_dates=True)
    preprocessed_provincial_data_df = preprocess_provincial_df(provincial_data_df)

    @task()
    def national_data_etl():
        records = preprocessed_national_data_df.to_dict(orient="records")
        nat_data_coll.drop()
        nat_data_coll.insert_many(records, ordered=True)

    @task()
    def national_trends_data_etl():
        national_trends = build_national_trends(preprocessed_national_data_df)
        nat_trends_coll.drop()
        nat_trends_coll.insert_many(national_trends)

    @task()
    def national_series_data_etl():
        national_series = build_national_series(preprocessed_national_data_df)
        nat_series_coll.drop()
        nat_series_coll.insert_one(national_series)

    @task()
    def regional_data_etl():
        """Drop and recreate regional data collection"""
        regional_records = preprocessed_regional_data_df.to_dict(orient="records")
        reg_data_coll.drop()
        reg_data_coll.insert_many(regional_records, ordered=True)

    @task()
    def regional_breakdown_data_etl():
        """Drop and recreate regional breakdown data collection"""
        regional_breakdown = build_regional_breakdown(preprocessed_regional_data_df)
        reg_breakdown_coll.drop()
        reg_breakdown_coll.insert_one(regional_breakdown)

    @task()
    def regional_series_data_etl():
        """Drop and recreate regional series data collection"""
        regional_series = build_regional_series(preprocessed_regional_data_df)
        reg_series_coll.drop()
        reg_series_coll.insert_many(regional_series)

    @task()
    def regional_trends_data_etl():
        """Drop and recreate regional trends data collection"""
        regional_trends = build_regional_trends(preprocessed_regional_data_df)
        reg_trends_coll.drop()
        reg_trends_coll.insert_many(regional_trends)

    @task()
    def provincial_data_etl():
        """Drop and recreate provincial collection"""
        provincial_records = preprocessed_provincial_data_df.to_dict(orient="records")
        prov_data_coll.drop()
        prov_data_coll.insert_many(provincial_records, ordered=True)

    @task
    def provincial_breakdown_data_etl():
        """Drop and create provincial breakdown collection"""
        provincial_breakdowns = build_provincial_breakdowns(
            preprocessed_provincial_data_df
        )
        prov_breakdown_coll.drop()
        prov_breakdown_coll.insert_many(provincial_breakdowns)

    @task()
    def provincial_series_data_etl():
        """Drop and recreate provincial series data collection"""
        provincial_series = build_provincial_series(preprocessed_provincial_data_df)
        prov_series_coll.drop()
        prov_series_coll.insert_many(provincial_series)

    @task()
    def provincial_trends_data_etl():
        """Create provincial trends data collection"""
        provincial_trends = build_provincial_trends(preprocessed_provincial_data_df)
        prov_trends_coll.drop()
        prov_trends_coll.insert_many(provincial_trends)

    national_data_etl() >> [national_trends_data_etl(), national_series_data_etl()]
    regional_data_etl() >> [
        regional_trends_data_etl(),
        regional_series_data_etl(),
        regional_breakdown_data_etl(),
    ]
    provincial_data_etl() >> [
        provincial_trends_data_etl(),
        provincial_series_data_etl(),
        provincial_breakdown_data_etl(),
    ]


DPC_ETL = pcm_dpc_etl()
