"""
OD Collections
"""
import os

from airflow.providers.mongo.hooks.mongo import MongoHook

mongo = MongoHook()

vax_admins_coll = mongo.get_collection(os.environ["AIRFLOW_VAR_VAX_ADMINS_COLLECTION"])
vax_admins_summary_coll = mongo.get_collection(
    os.environ["AIRFLOW_VAR_VAX_ADMINS_SUMMARY_COLLECTION"]
)
pop_coll = mongo.get_collection(os.environ["AIRFLOW_VAR_POP_COLLECTION"])
