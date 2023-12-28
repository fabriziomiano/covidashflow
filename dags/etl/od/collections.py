"""
OD Collections
"""
from airflow.models import Variable
from utils.common import get_logger
from utils.db import mongo_hook

logger = get_logger("od-collections")

vax_admins_coll_name = Variable.get("VAX_ADMINS_COLLECTION")
logger.info(f"Getting {vax_admins_coll_name}")
vax_admins_coll = mongo_hook.get_collection(vax_admins_coll_name)
logger.info(f"Loaded {vax_admins_coll}")

vax_admins_summary_coll_name = Variable.get("VAX_ADMINS_SUMMARY_COLLECTION")
logger.info(f"Getting {vax_admins_summary_coll_name}")
vax_admins_summary_coll = mongo_hook.get_collection(vax_admins_summary_coll_name)
logger.info(f"Loaded {vax_admins_summary_coll}")

pop_coll_name = Variable.get("POP_COLLECTION")
logger.info(f"Getting {pop_coll_name}")
pop_coll = mongo_hook.get_collection(pop_coll_name)
logger.info(f"Loaded {pop_coll}")
