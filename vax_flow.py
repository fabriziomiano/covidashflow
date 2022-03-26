import pandas as pd
from airflow.decorators import dag, task

from ETL import (
    vax_admins_coll, vax_admins_summary_coll, pop_coll
)
from ETL.etl import (
    preprocess_vax_admins_df, preprocess_vax_admins_summary_df
)
from settings import DEFAULT_DAG_ARGS
from settings.urls import (
    URL_VAX_ADMINS_DATA, URL_VAX_ADMINS_SUMMARY_DATA, URL_VAX_POP_DATA
)
from settings.vars import VAX_DATE_KEY


@dag('OpenData-ETL', catchup=False, tags=['COVID'], default_args=DEFAULT_DAG_ARGS)
def od_etl():
    """
    """

    @task()
    def load_vax_admins_data():
        """Create vaccine administrations colleciton"""
        df = pd.read_csv(
            URL_VAX_ADMINS_DATA, parse_dates=[VAX_DATE_KEY], low_memory=False)
        df = preprocess_vax_admins_df(df)
        records = df.to_dict(orient='records')
        vax_admins_coll.drop()
        vax_admins_coll.insert_many(records, ordered=True)

    @task()
    def load_vax_admins_summary_data():
        """Create vaccine administrations summary colleciton"""
        df = pd.read_csv(
            URL_VAX_ADMINS_SUMMARY_DATA, parse_dates=[VAX_DATE_KEY])
        df = preprocess_vax_admins_summary_df(df)
        records = df.to_dict(orient='records')
        vax_admins_summary_coll.drop()
        vax_admins_summary_coll.insert_many(records, ordered=True)

    @task()
    def load_vax_pop_data():
        """Create OD population collection"""
        pop_df = pd.read_csv(URL_VAX_POP_DATA)
        records = pop_df.to_dict(orient='records')
        pop_coll.drop()
        pop_coll.insert_many(records)

    load_vax_admins_data()
    load_vax_admins_summary_data()
    load_vax_pop_data()


VAX_ETL = od_etl()
