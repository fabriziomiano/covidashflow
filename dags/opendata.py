"""Airflow DAG for loading Italia Open Data vaccination datasets into MongoDB."""

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.mongo.hooks.mongo import MongoHook

from covidashflow.common.mongo import collections_from_airflow, replace_collection
from covidashflow.common.urls import (
    URL_VAX_ADMINS_DATA,
    URL_VAX_ADMINS_SUMMARY_DATA,
    URL_VAX_POP_DATA,
)
from covidashflow.opendata.pipeline import extract_opendata_frame
from covidashflow.opendata.transform import (
    get_region_pop_dict,
    preprocess_vax_admins_df,
    preprocess_vax_admins_summary_df,
)


def get_collections():
    """Resolve Mongo collections from Airflow variables for task execution."""
    return collections_from_airflow(MongoHook("MONGO_DEFAULT"), Variable)


@dag(
    dag_id=Variable.get("OD_DAG_ID", default_var="italia-covid19-opendata-etl"),
    catchup=False,
    tags=["italia", "covid", "vaccines", "opendata"],
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=Variable.get("OD_SCHEDULE_INTERVAL", default_var="@daily"),
)
def opendata_etl():
    """Define the vaccination and population ETL workflow."""

    @task()
    def copy_population_data():
        """Extract and load the population reference dataset."""
        collections = get_collections()
        population_df = extract_opendata_frame(URL_VAX_POP_DATA)
        replace_collection(collections.population, population_df.to_dict(orient="records"))

    @task()
    def extract_vax_data():
        """Extract vaccination administration records."""
        return extract_opendata_frame(
            URL_VAX_ADMINS_DATA,
            parse_dates=True,
            low_memory=False,
        )

    @task()
    def transform_vax_data(vax_admins_df):
        """Transform vaccination administration records."""
        return preprocess_vax_admins_df(vax_admins_df)

    @task()
    def load_vax_data(preprocessed_vax_df):
        """Load vaccination administration records into MongoDB."""
        collections = get_collections()
        replace_collection(
            collections.vax_admins,
            preprocessed_vax_df.to_dict(orient="records"),
            ordered=True,
        )

    @task()
    def extract_vax_summary_data():
        """Extract vaccination summary records."""
        return extract_opendata_frame(URL_VAX_ADMINS_SUMMARY_DATA, parse_dates=True)

    @task()
    def transform_vax_summary_data(vax_admins_summary_df):
        """Transform vaccination summary records and attach population totals."""
        collections = get_collections()
        population = get_region_pop_dict(collections.population)
        return preprocess_vax_admins_summary_df(vax_admins_summary_df, population)

    @task()
    def load_vax_summary_data(preprocessed_summary_df):
        """Load vaccination summary records into MongoDB."""
        collections = get_collections()
        replace_collection(
            collections.vax_admins_summary,
            preprocessed_summary_df.to_dict(orient="records"),
            ordered=True,
        )

    population = copy_population_data()

    vax_data = extract_vax_data()
    preprocessed_vax_data = transform_vax_data(vax_data)
    load_vax_data(preprocessed_vax_data)

    summary_data = extract_vax_summary_data()
    preprocessed_summary = transform_vax_summary_data(summary_data)
    population >> preprocessed_summary
    load_vax_summary_data(preprocessed_summary)


OD_ETL = opendata_etl()
