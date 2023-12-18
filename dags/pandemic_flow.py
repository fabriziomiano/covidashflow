import pandas as pd
from airflow.decorators import dag, task
from ETL import (
    nat_data_coll,
    nat_series_coll,
    nat_trends_coll,
    prov_bdown_coll,
    prov_data_coll,
    prov_series_coll,
    prov_trends_coll,
    reg_bdown_coll,
    reg_data_coll,
    reg_series_coll,
    reg_trends_coll,
    vax_admins_coll,
)
from ETL.etl import (
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
    preprocess_vax_admins_df,
)
from settings import DEFAULT_DAG_ARGS
from settings.urls import (
    URL_NATIONAL,
    URL_PROVINCIAL,
    URL_REGIONAL,
    URL_VAX_ADMINS_DATA,
)
from settings.vars import DATE_KEY, VAX_DATE_KEY


@dag("DPC-ETL", catchup=False, tags=["COVID"], default_args=DEFAULT_DAG_ARGS)
def pcm_dpc_etl():
    """ """

    @task()
    def load_national_data():
        df_national = pd.read_csv(URL_NATIONAL, parse_dates=[DATE_KEY])
        df_national.drop(columns=COLUMNS_TO_DROP, inplace=True)
        df_national = preprocess_national_df(df_national)
        national_records = df_national.to_dict(orient="records")
        nat_data_coll.drop()
        nat_data_coll.insert_many(national_records, ordered=True)

    @task()
    def load_national_trends_data():
        df = pd.read_csv(URL_NATIONAL, parse_dates=[DATE_KEY])
        df.drop(columns=COLUMNS_TO_DROP, inplace=True)
        df_national_augmented = preprocess_national_df(df)
        national_trends = build_national_trends(df_national_augmented)
        nat_trends_coll.drop()
        nat_trends_coll.insert_many(national_trends)

    @task()
    def load_national_series_data():
        df = pd.read_csv(URL_NATIONAL, parse_dates=[DATE_KEY])
        df.drop(columns=COLUMNS_TO_DROP, inplace=True)
        df_national_augmented = preprocess_national_df(df)
        national_series = build_national_series(df_national_augmented)
        nat_series_coll.drop()
        nat_series_coll.insert_one(national_series)

    @task()
    def load_regional_data():
        """Drop and recreate regional data collection"""
        df = pd.read_csv(URL_REGIONAL, parse_dates=[DATE_KEY])
        df.drop(columns=COLUMNS_TO_DROP, inplace=True)
        df_regional_augmented = preprocess_regional_df(df)
        regional_records = df_regional_augmented.to_dict(orient="records")
        reg_data_coll.drop()
        reg_data_coll.insert_many(regional_records, ordered=True)

    @task()
    def load_regional_breakdown_data():
        """Drop and recreate regional breakdown data collection"""
        df = pd.read_csv(URL_REGIONAL, parse_dates=[DATE_KEY], low_memory=False)
        df.drop(columns=COLUMNS_TO_DROP, inplace=True)
        df_regional_augmented = preprocess_regional_df(df)
        regional_breakdown = build_regional_breakdown(df_regional_augmented)
        reg_bdown_coll.drop()
        reg_bdown_coll.insert_one(regional_breakdown)

    @task()
    def load_regional_series_data():
        """Drop and recreate regional series data collection"""
        df = pd.read_csv(URL_REGIONAL, parse_dates=[DATE_KEY], low_memory=False)
        df.drop(columns=COLUMNS_TO_DROP, inplace=True)
        df_regional_augmented = preprocess_regional_df(df)
        regional_series = build_regional_series(df_regional_augmented)
        reg_series_coll.drop()
        reg_series_coll.insert_many(regional_series)

    @task()
    def load_regional_trends_data():
        """Drop and recreate regional trends data collection"""
        df = pd.read_csv(URL_REGIONAL, parse_dates=[DATE_KEY], low_memory=False)
        df.drop(columns=COLUMNS_TO_DROP, inplace=True)
        df_regional_augmented = preprocess_regional_df(df)
        regional_trends = build_regional_trends(df_regional_augmented)
        reg_trends_coll.drop()
        reg_trends_coll.insert_many(regional_trends)

    @task()
    def load_provincial_data():
        """Drop and recreate provincial collection"""
        df = pd.read_csv(URL_PROVINCIAL)
        df = df.rename(columns=lambda x: x.strip())
        df[DATE_KEY] = pd.to_datetime(df[DATE_KEY])
        df.drop(columns=COLUMNS_TO_DROP, inplace=True)
        df_provincial_augmented = preprocess_provincial_df(df)
        provincial_records = df_provincial_augmented.to_dict(orient="records")
        prov_data_coll.drop()
        prov_data_coll.insert_many(provincial_records, ordered=True)

    @task
    def load_provincial_breakdown_data():
        """Drop and create provincial breakdown collection"""
        df = pd.read_csv(URL_PROVINCIAL)
        df = df.rename(columns=lambda x: x.strip())
        df[DATE_KEY] = pd.to_datetime(df[DATE_KEY])
        df.drop(columns=COLUMNS_TO_DROP, inplace=True)
        df_provincial_augmented = preprocess_provincial_df(df)
        provincial_breakdowns = build_provincial_breakdowns(df_provincial_augmented)
        prov_bdown_coll.drop()
        prov_bdown_coll.insert_many(provincial_breakdowns)

    @task()
    def load_provincial_series_data():
        """Drop and recreate provincial series data collection"""
        df = pd.read_csv(URL_PROVINCIAL)
        df = df.rename(columns=lambda x: x.strip())
        df[DATE_KEY] = pd.to_datetime(df[DATE_KEY])
        df.drop(columns=COLUMNS_TO_DROP, inplace=True)
        df_provincial_augmented = preprocess_provincial_df(df)
        provincial_series = build_provincial_series(df_provincial_augmented)
        prov_series_coll.drop()
        prov_series_coll.insert_many(provincial_series)

    @task()
    def load_provincial_trends_data():
        """Create provincial trends data collection"""
        df = pd.read_csv(URL_PROVINCIAL)
        df = df.rename(columns=lambda x: x.strip())
        df[DATE_KEY] = pd.to_datetime(df[DATE_KEY])
        df.drop(columns=COLUMNS_TO_DROP, inplace=True)
        df_provincial_augmented = preprocess_provincial_df(df)
        provincial_trends = build_provincial_trends(df_provincial_augmented)
        prov_trends_coll.drop()
        prov_trends_coll.insert_many(provincial_trends)

    @task()
    def load_vax_admins_data():
        """Create vaccine administrations colleciton"""
        df = pd.read_csv(
            URL_VAX_ADMINS_DATA, parse_dates=[VAX_DATE_KEY], low_memory=False
        )
        df = preprocess_vax_admins_df(df)
        records = df.to_dict(orient="records")
        vax_admins_coll.drop()
        vax_admins_coll.insert_many(records, ordered=True)

    load_national_data() >> [load_national_trends_data(), load_national_series_data()]
    load_regional_data() >> [
        load_regional_trends_data(),
        load_regional_series_data(),
        load_regional_breakdown_data(),
    ]
    load_provincial_data() >> [
        load_provincial_trends_data(),
        load_provincial_series_data(),
        load_provincial_breakdown_data(),
    ]


DPC_ETL = pcm_dpc_etl()
