from utils.common import get_logger

logger = get_logger("OD-load")


def load_preprocessed_od_df_to_mongo(df, collection, data_type, ordered=False, batch_size=1000):
    """
    Loads preprocessed data from a Pandas DataFrame into a MongoDB collection in batches.

    This function converts a Pandas DataFrame to a list of dictionaries and inserts the records into
    the specified MongoDB collection in batches to manage memory usage and improve performance.
    The collection is dropped before loading new data.

    Parameters:
    df (pandas.DataFrame): The DataFrame containing the preprocessed data to be inserted into MongoDB.
    collection (pymongo.collection.Collection): The MongoDB collection where the records will be inserted.
    data_type (str): A string indicating the type of data being loaded, used for logging purposes.
    ordered (bool, optional): If True, MongoDB will stop inserting at the first error. If False, MongoDB will continue with other inserts when an error occurs. Default is False.
    batch_size (int, optional): The number of records to be inserted in each batch. Default is 1000.

    Returns:
    None

    Logs the progress and results of the insertion, including the number of records inserted in each batch.
    """

    records = df.to_dict(orient="records")
    logger.info(f"Batch loading {len(records)} records of type {data_type} to {collection}")
    collection.drop()

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            collection.insert_many(batch, ordered=ordered)
            logger.info(f"Inserted batch {i // batch_size + 1}")
        except Exception as e:
            print(f"Unexpected error inserting batch {i // batch_size + 1}: {e}")
