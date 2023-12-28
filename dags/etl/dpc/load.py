from utils.common import get_logger

logger = get_logger("DPC-load")


def load_dpc_records_to_mongo(records, collection, data_type, ordered=False):
    logger.info(f"Loading {data_type} to {collection}")
    collection.drop()
    result_many = collection.insert_many(records, ordered=ordered)
    logger.info(f"Landed {len(result_many.inserted_ids)} records")
