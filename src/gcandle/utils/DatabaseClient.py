import json

import pandas as pd
import pymongo
import gcandle.utils.date_time_utils as date_time_utils
from gcandle.utils.gcandle_config import GCANDLE_CONFIG

READ_BATCH_SIZE = 100000
DEFAULT_PRECISION = 5


def to_json(data):
    if 'date' in data.columns:
        data.date = data.date.apply(lambda x: str(x)[0:10])
    if 'datetime' in data.columns:
        data.datetime = data.datetime.apply(lambda x: str(x)[0:19])
    json_data = json.loads(data.to_json(orient='records'))
    return json_data


class DatabaseClient:
    def __init__(self):
        self.connection = pymongo.MongoClient = None
        self.dbName = GCANDLE_CONFIG.mongodb_name

    def set_dbName(self, name):
        self.dbName = name
        return self

    def connect(self):
        self.connection = pymongo.MongoClient(GCANDLE_CONFIG.mongodb_uri)

    def get_client(self):
        if self.connection is None:
            self.connect()
        return self.connection[self.dbName]

    # Multiprocess init can call this function so that every process have their own mongodb connection.
    def init(self, name=GCANDLE_CONFIG.mongodb_name):
        self.connect()
        return self


