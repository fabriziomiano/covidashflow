from utils.common import get_logger

logger = get_logger("DPC-load")


def load_dpc_records_to_mongo(
        records,
        collection,
        data_type,
        ordered=False,
        batch_size=1000
):
    """
    Loads records into a MongoDB collection in batches.

    This function inserts records from a provided list into the specified MongoDB collection.
    It processes the data in batches to manage memory usage and improve performance.
    The collection is dropped before loading new data.

    Parameters:
    data (list): A list of dictionaries representing the records to be inserted into MongoDB.
    collection (pymongo.collection.Collection): The MongoDB collection where the records will be inserted.
    data_type (str): A string indicating the type of data being loaded, used for logging purposes.
    ordered (bool, optional): If True, MongoDB will stop inserting at the first error. If False, MongoDB will continue with other inserts when an error occurs. Default is False.
    batch_size (int, optional): The number of records to be inserted in each batch. Default is 1000.

    Returns:
    None

    Logs the progress and results of the batch insertion, including the number of records inserted in each batch.
    """

    logger.info(f"Batch loading {len(records)} records of type {data_type} to {collection}")
    collection.drop()
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            collection.insert_many(batch, ordered=ordered)
            logger.info(f"Inserted batch {i // batch_size + 1}")
        except Exception as e:
            print(f"Unexpected error inserting batch {i // batch_size + 1}: {e}")