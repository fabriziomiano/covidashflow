"""Common Mongo utils"""

import os

from airflow.providers.mongo.hooks.mongo import MongoHook

conn_id = os.environ["AIRFLOW_CONN_MONGO_DEFAULT"]
mongo_hook = MongoHook()
