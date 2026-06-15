"""Airflow DAG for loading PCM-DPC COVID-19 datasets into MongoDB."""

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook

from covidashflow.common.mongo import collections_from_airflow
from covidashflow.common.urls import URL_NATIONAL, URL_PROVINCIAL, URL_REGIONAL
from covidashflow.dpc.pipeline import (
    extract_dpc_frame,
    load_national_outputs,
    load_provincial_outputs,
    load_regional_outputs,
)
from covidashflow.dpc.transform import (
    preprocess_national_df,
    preprocess_provincial_df,
    preprocess_regional_df,
)


def get_collections():
    """Resolve Mongo collections from Airflow variables for task execution."""
    return collections_from_airflow(MongoHook("MONGO_DEFAULT"), Variable)


@dag(
    dag_id=Variable.get("DPC_DAG_ID", default_var="pcm-dpc-covid19-etl"),
    catchup=False,
    tags=["italia", "dpc-pcm", "covid"],
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=Variable.get("DPC_SCHEDULE_INTERVAL", default_var="@weekly"),
)
def pcm_dpc_etl():
    """Define the PCM-DPC national, regional, and provincial ETL workflow."""

    @task()
    def extract_national_data():
        """Extract the national PCM-DPC CSV dataset."""
        return extract_dpc_frame(URL_NATIONAL, parse_dates=True)

    @task()
    def transform_national_data(national_df):
        """Transform national PCM-DPC records."""
        return preprocess_national_df(national_df)

    @task()
    def load_national_data(preprocessed_df):
        """Load national PCM-DPC outputs into MongoDB."""
        load_national_outputs(preprocessed_df, get_collections())

    @task()
    def extract_regional_data():
        """Extract the regional PCM-DPC CSV dataset."""
        return extract_dpc_frame(URL_REGIONAL, parse_dates=True)

    @task()
    def transform_regional_data(regional_df):
        """Transform regional PCM-DPC records."""
        return preprocess_regional_df(regional_df)

    @task()
    def load_regional_data(preprocessed_df):
        """Load regional PCM-DPC outputs into MongoDB."""
        load_regional_outputs(preprocessed_df, get_collections())

    @task()
    def extract_provincial_data():
        """Extract the provincial PCM-DPC CSV dataset."""
        return extract_dpc_frame(URL_PROVINCIAL, parse_dates=True)

    @task()
    def transform_provincial_data(provincial_df):
        """Transform provincial PCM-DPC records."""
        return preprocess_provincial_df(provincial_df)

    @task()
    def load_provincial_data(preprocessed_df):
        """Load provincial PCM-DPC outputs into MongoDB."""
        load_provincial_outputs(preprocessed_df, get_collections())

    national_data = extract_national_data()
    transformed_national_data = transform_national_data(national_data)
    load_national_data(transformed_national_data)

    regional_data = extract_regional_data()
    transformed_regional_data = transform_regional_data(regional_data)
    load_regional_data(transformed_regional_data)

    provincial_data = extract_provincial_data()
    transformed_provincial_data = transform_provincial_data(provincial_data)
    load_provincial_data(transformed_provincial_data)


DPC_ETL = pcm_dpc_etl()
