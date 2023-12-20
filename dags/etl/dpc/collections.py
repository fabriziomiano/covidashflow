"""
DPC Collections
"""
import os

from utils.db import mongo_hook

nat_data_coll_name = os.environ["AIRFLOW_VAR_NATIONAL_DATA_COLLECTION"]
nat_data_coll = mongo_hook.get_collection(nat_data_coll_name)

nat_trend_coll_name = os.environ["AIRFLOW_VAR_NATIONAL_TRENDS_COLLECTION"]
nat_trends_coll = mongo_hook.get_collection(nat_trend_coll_name)

nat_series_coll_name = os.environ["AIRFLOW_VAR_NATIONAL_SERIES_COLLECTION"]
nat_series_coll = mongo_hook.get_collection(nat_series_coll_name)

reg_data_coll_name = os.environ["AIRFLOW_VAR_REGIONAL_DATA_COLLECTION"]
reg_data_coll = mongo_hook.get_collection(reg_data_coll_name)

reg_trends_coll_name = os.environ["AIRFLOW_VAR_REGIONAL_TRENDS_COLLECTION"]
reg_trends_coll = mongo_hook.get_collection(reg_trends_coll_name)

reg_series_coll_name = os.environ["AIRFLOW_VAR_REGIONAL_SERIES_COLLECTION"]
reg_series_coll = mongo_hook.get_collection(reg_series_coll_name)

reg_breakdown_coll_name = os.environ["AIRFLOW_VAR_REGIONAL_BREAKDOWN_COLLECTION"]
reg_breakdown_coll = mongo_hook.get_collection(reg_breakdown_coll_name)

prov_data_coll_name = os.environ["AIRFLOW_VAR_PROVINCIAL_DATA_COLLECTION"]
prov_data_coll = mongo_hook.get_collection(prov_data_coll_name)

prov_trends_coll_name = os.environ["AIRFLOW_VAR_PROVINCIAL_TRENDS_COLLECTION"]
prov_trends_coll = mongo_hook.get_collection(prov_trends_coll_name)

prov_series_coll_name = os.environ["AIRFLOW_VAR_PROVINCIAL_SERIES_COLLECTION"]
prov_series_coll = mongo_hook.get_collection(prov_series_coll_name)

prov_breakdown_coll_name = os.environ["AIRFLOW_VAR_PROVINCIAL_BREAKDOWN_COLLECTION"]
prov_breakdown_coll = mongo_hook.get_collection(prov_breakdown_coll_name)
