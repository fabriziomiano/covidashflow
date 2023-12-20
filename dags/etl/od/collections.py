"""
OD Collections
"""
import os

from utils.db import mongo_hook

vax_admins_coll_name = os.environ["AIRFLOW_VAR_VAX_ADMINS_COLLECTION"]
vax_admins_coll = mongo_hook.get_collection(vax_admins_coll_name)

vax_admins_summary_coll_name = os.environ["AIRFLOW_VAR_VAX_ADMINS_SUMMARY_COLLECTION"]
vax_admins_summary_coll = mongo_hook.get_collection(vax_admins_summary_coll_name)

pop_coll_name = os.environ["AIRFLOW_VAR_POP_COLLECTION"]
pop_coll = mongo_hook.get_collection(pop_coll_name)
