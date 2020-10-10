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
        self.client: pymongo.MongoClient = None

    def set_real_client(self, client):
        self.client = client

    def get_mongo_connection(self):
        return pymongo.MongoClient(GCANDLE_CONFIG.mongodb_uri)

    def get_database(self):
        return self.get_mongo_connection()[GCANDLE_CONFIG.mongodb_name]

    # Multiprocess init can call this function so that every process have their own mongodb connection.
    def init(self):
        self.client = self.get_database()
        return self

    def ensure_client(self):
        if self.client is None:
            self.init()

    def create_index(self, repo_name, keys_list):
        self.ensure_client()
        coll = self.client[repo_name]
        if len(keys_list) > 0:
            for k in keys_list:
                coll.create_index([(k, pymongo.DESCENDING)])
        else:
            print('No keys provided for creating index, NOOP')

    def create_index_for_update_time(self, repo_name):
        self.ensure_client()
        coll = self.client[repo_name]
        coll.create_index(
            [
                ('update_time', pymongo.DESCENDING)
            ]
        )

    def create_index_for_collection_with_date_and_code(self, repo_name):
        self.ensure_client()
        coll = self.client[repo_name]
        coll.create_index(
            [
                ('code', pymongo.ASCENDING),
            ]
        )
        coll.create_index(
            [
                ('date', pymongo.DESCENDING)
            ]
        )
        coll.create_index(
            [
                ('update_time', pymongo.DESCENDING)
            ]
        )
        coll.create_index(
        [
            ('code', pymongo.ASCENDING),
            ('date', pymongo.DESCENDING)
        ], unique=True
        )

    def count_of(self, repo_name, query = None):
        self.ensure_client()
        if query is not None:
            cnt = self.client[repo_name].count_documents(query)
        else:
            cnt = self.client[repo_name].count_documents({})
        return cnt

    def replace_data_with_its_date_range(self, data, repo_name, precision=DEFAULT_PRECISION):
        self.ensure_client()
        if data is None or len(data) < 1:
            return
        data = data.round(precision).reset_index()
        start = data.date.min()
        end = data.date.max()
        code = data.code[0]
        self.delete_by_code_and_dates(repo_name, code, start, end)
        self._insert_data(data, repo_name)

    def replace_data_by_date_range(self, data, repo_name, start, end, precision=DEFAULT_PRECISION):
        self.ensure_client()
        if data is None or len(data) < 1:
            return
        data = data.round(precision).reset_index()
        code = data.code[0]
        self.delete_by_code_and_dates(repo_name, code, start, end)
        self._insert_data(data, repo_name)

    def delete_by_code_and_dates(self, repo_name, code, start, end):
        coll = self.client[repo_name]
        coll.delete_many({'code': {'$in': [code]}, 'date': {
            '$gte': start, '$lte': end
        }})

    def _insert_data(self, data, repo_name):
        coll = self.client[repo_name]
        data['update_time'] = date_time_utils.Date().as_str_with_time()
        return coll.insert_many(to_json(data))

    def replace_data_by_codes(self, data, repo_name, precision=DEFAULT_PRECISION):
        self.ensure_client()
        if data is None or len(data) < 1:
            return
        data = data.round(precision).reset_index()
        codes = data.code.unique().tolist()
        coll = self.client[repo_name]
        coll.delete_many({'code': {'$in': codes}})
        if len(data) != 0:
            data['update_time'] = date_time_utils.Date().as_str_with_time()
            return coll.insert_many(to_json(data))

    def update_data_append_newer(self, data, repo_name, precision=DEFAULT_PRECISION):
        self.ensure_client()
        if data is None or len(data) < 1:
            return
        data = data.round(precision).reset_index()
        codes = data.code.unique()
        keep_flag = 'newer_to_keep'
        data[keep_flag] = False
        for code in codes:
            max_date = self.read_max_date_for(code, repo_name)
            code_filter = data.code == code
            if max_date is None:
                data.loc[code_filter, keep_flag] = True
            elif len(data.loc[code_filter & (data.date > max_date)]) > 0:
                data.loc[code_filter & (data.date > max_date), keep_flag] = True
        data = data.loc[data[keep_flag] == True]
        if data is not None and len(data) > 0:
            data['update_time'] = date_time_utils.Date().as_str_with_time()
            data.drop(columns=[keep_flag], inplace=True)
            self.client[repo_name].insert_many(to_json(data))

    def read_codes_from_db(self, repo_name, filter=None):
        self.ensure_client()
        db = self.client[repo_name]
        res = db.distinct('code', filter=filter)
        return res

    def construct_dataframe(self, cursor):
        self.ensure_client()
        res = pd.DataFrame([item for item in cursor])
        if res.shape[0] < 1:
            return None
        try:
            if 'datetime' in res.columns:
                time_index = 'datetime'
            else:
                time_index = 'date'
            res = res.assign(date=pd.to_datetime(
                res.date)).drop_duplicates(([time_index, 'code']))
            res['code'] = res['code'].astype(str)
            res = res.set_index([time_index, 'code'], drop=True).sort_index(level=0)
        except:
            print('Cannot convert data from db to dataframe')
            res = None
        return res

    def read_all_data(self, repo_name):
        self.ensure_client()
        coll = self.client[repo_name]
        cursor = coll.find({}, {'_id': 0}, batch_size=READ_BATCH_SIZE)
        return self.construct_dataframe(cursor)

    def read_data_by_codes_and_date(
            self,
            repo_name,
            codes,
            start, end=None,
            extra_filter=None,
            ):
        if end is None:
            end = date_time_utils.Date().as_str()
        else:
            end = str(end)[0:10]
        start = str(start)[0:10]

        query = {
            'code': {'$in': codes},
            "date": {
                "$lte": end,
                "$gte": start
            }
        }
        if extra_filter is not None:
            query.update(extra_filter)
        coll = self.client[repo_name]
        cursor = coll.find(query, {"_id": 0, 'update_time': 0, 'date_stamp': 0}, batch_size=READ_BATCH_SIZE)
        return self.construct_dataframe(cursor)

    def read_max_date_for(
            self, code, repo_name
            ):
        self.ensure_client()
        repo = self.client[repo_name]
        cnt = repo.count_documents({'code': code})
        if cnt < 1:
            return None
        end = repo.find({'code': code}, {'date': 1, '_id': 0}).sort('date', pymongo.DESCENDING).limit(1)[0]['date']
        return end[0:10]

    def remove_all_from(self, repo_name, query=None):
        self.ensure_client()
        repo = self.client[repo_name]
        if query is None:
            repo.drop()
        else:
            repo.remove(query)

    def drop(self, repo_name):
        self.ensure_client()
        repo = self.client[repo_name]
        repo.drop()

