"""Explicit insert-only teaching fixture seed; existing documents are retained."""
from pathlib import Path
from bson import json_util
from airflow.hooks.base import BaseHook
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern

connection = BaseHook.get_connection('task3_mongo')
with MongoClient(connection.get_uri(), serverSelectionTimeoutMS=5000, socketTimeoutMS=8000) as client:
    db = client.get_database(connection.schema, write_concern=WriteConcern('majority', wtimeout=5000))
    for name in ('clients', 'orders', 'restaurant'):
        if name not in db.list_collection_names():
            raise RuntimeError('Run explicit task3-mongo-init first')
        documents = json_util.loads((Path('/opt/airflow/data/task3') / (name + '.json')).read_text())
        for document in documents:
            db[name].update_one({'_id': document['_id']}, {'$setOnInsert': document}, upsert=True)
        print(name, 'fixture rows checked:', len(documents))
