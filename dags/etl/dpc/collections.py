"""
DPC Collections
"""
import os

from airflow.providers.mongo.hooks.mongo import MongoHook

mongo = MongoHook()

nat_data_coll = mongo.get_collection(os.environ["AIRFLOW_VAR_NATIONAL_DATA_COLLECTION"])
nat_trends_coll = mongo.get_collection(
    os.environ["AIRFLOW_VAR_NATIONAL_TRENDS_COLLECTION"]
)
nat_series_coll = mongo.get_collection(
    os.environ["AIRFLOW_VAR_NATIONAL_SERIES_COLLECTION"]
)
reg_data_coll = mongo.get_collection(os.environ["AIRFLOW_VAR_REGIONAL_DATA_COLLECTION"])
reg_trends_coll = mongo.get_collection(
    os.environ["AIRFLOW_VAR_REGIONAL_TRENDS_COLLECTION"]
)
reg_series_coll = mongo.get_collection(
    os.environ["AIRFLOW_VAR_REGIONAL_SERIES_COLLECTION"]
)
reg_bdown_coll = mongo.get_collection(
    os.environ["AIRFLOW_VAR_REGIONAL_BREAKDOWN_COLLECTION"]
)
prov_data_coll = mongo.get_collection(
    os.environ["AIRFLOW_VAR_PROVINCIAL_DATA_COLLECTION"]
)
prov_trends_coll = mongo.get_collection(
    os.environ["AIRFLOW_VAR_PROVINCIAL_TRENDS_COLLECTION"]
)
prov_series_coll = mongo.get_collection(
    os.environ["AIRFLOW_VAR_PROVINCIAL_SERIES_COLLECTION"]
)
prov_bdown_coll = mongo.get_collection(
    os.environ["AIRFLOW_VAR_PROVINCIAL_BREAKDOWN_COLLECTION"]
)
