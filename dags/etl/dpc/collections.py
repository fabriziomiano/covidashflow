"""
DPC Collections
"""
from airflow.models import Variable
from utils.common import get_logger
from utils.db import mongo_hook

logger = get_logger("dpc-collections")


nat_data_coll_name = Variable.get("NATIONAL_DATA_COLLECTION")
logger.info(f"Getting collection {nat_data_coll_name}")
nat_data_coll = mongo_hook.get_collection(nat_data_coll_name)
logger.info(f"Loaded {nat_data_coll}")

nat_trend_coll_name = Variable.get("NATIONAL_TRENDS_COLLECTION")
logger.info(f"Getting collection {nat_trend_coll_name}")
nat_trends_coll = mongo_hook.get_collection(nat_trend_coll_name)
logger.info(f"Loaded {nat_trends_coll}")

nat_series_coll_name = Variable.get("NATIONAL_SERIES_COLLECTION")
logger.info(f"Getting collection {nat_series_coll_name}")
nat_series_coll = mongo_hook.get_collection(nat_series_coll_name)
logger.info(f"Loaded {nat_series_coll}")

reg_data_coll_name = Variable.get("REGIONAL_DATA_COLLECTION")
logger.info(f"Getting collection {reg_data_coll_name}")
reg_data_coll = mongo_hook.get_collection(reg_data_coll_name)
logger.info(f"Loaded {reg_data_coll}")

reg_trends_coll_name = Variable.get("REGIONAL_TRENDS_COLLECTION")
logger.info(f"Getting collection {reg_trends_coll_name}")
reg_trends_coll = mongo_hook.get_collection(reg_trends_coll_name)
logger.info(f"Loaded {reg_trends_coll}")

reg_series_coll_name = Variable.get("REGIONAL_SERIES_COLLECTION")
logger.info(f"Getting collection {reg_series_coll_name}")
reg_series_coll = mongo_hook.get_collection(reg_series_coll_name)
logger.info(f"Loaded {reg_series_coll}")

reg_breakdown_coll_name = Variable.get("REGIONAL_BREAKDOWN_COLLECTION")
logger.info(f"Getting collection {reg_breakdown_coll_name}")
reg_breakdown_coll = mongo_hook.get_collection(reg_breakdown_coll_name)
logger.info(f"Loaded {reg_breakdown_coll}")

prov_data_coll_name = Variable.get("PROVINCIAL_DATA_COLLECTION")
logger.info(f"Getting collection {prov_data_coll_name}")
prov_data_coll = mongo_hook.get_collection(prov_data_coll_name)
logger.info(f"Loaded {prov_data_coll}")

prov_trends_coll_name = Variable.get("PROVINCIAL_TRENDS_COLLECTION")
logger.info(f"Getting collection {prov_trends_coll_name}")
prov_trends_coll = mongo_hook.get_collection(prov_trends_coll_name)
logger.info(f"Loaded {prov_trends_coll}")

prov_series_coll_name = Variable.get("PROVINCIAL_SERIES_COLLECTION")
logger.info(f"Getting collection {prov_series_coll_name}")
prov_series_coll = mongo_hook.get_collection(prov_series_coll_name)
logger.info(f"Loaded {prov_series_coll}")

prov_breakdown_coll_name = Variable.get("PROVINCIAL_BREAKDOWN_COLLECTION")
logger.info(f"Getting collection {prov_breakdown_coll_name}")
prov_breakdown_coll = mongo_hook.get_collection(prov_breakdown_coll_name)
logger.info(f"Loaded {prov_breakdown_coll}")
