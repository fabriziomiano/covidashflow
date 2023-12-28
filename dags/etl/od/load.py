from utils.common import get_logger

logger = get_logger("OD-load")


def load_preprocessed_od_df_to_mongo(df, collection, data_type, ordered=False):
    logger.info(f"Loading {data_type} to {collection}")
    records = df.to_dict(orient="records")
    collection.drop()
    result_many = collection.insert_many(records, ordered=ordered)
    logger.info(f"Landed {len(result_many.inserted_ids)} records")
