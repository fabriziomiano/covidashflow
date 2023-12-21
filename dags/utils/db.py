"""Common Mongo utils"""

from airflow.providers.mongo.hooks.mongo import MongoHook

mongo_hook = MongoHook("MONGO_DEFAULT")
