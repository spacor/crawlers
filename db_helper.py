import pymongo as pm
import logging
from collections import OrderedDict
import datetime as dt

import configs

script_name = 'db_helper'
log = logging.getLogger(script_name)

def to_db(as_of_dt, collection_name, daily_paper):
    try:
        log.debug('Inserting paper as of date {}'.format(as_of_dt))
        client = pm.MongoClient(configs.db_conn_str)
        db=client.crawls
        apple_daily_coll = db[collection_name]
        docu = OrderedDict([
            ('as_of_date', as_of_dt),
            ('crawl_date', dt.datetime.utcnow()),
            ('articles', daily_paper)
        ])
        db_results = apple_daily_coll.insert_one(docu)
        log.debug(db_results)
    except:
        log.error('Failed to insert data for {}'.format(as_of_dt))

def get_crawled_dt(collection_name):
    client = pm.MongoClient(configs.db_conn_str)
    db = client.crawls
    apple_daily_coll = db[collection_name]
    query = apple_daily_coll.find({}, {'as_of_date':1})
    crawled_dt = [i['as_of_date'] for i in query]
    return crawled_dt
