# -*- coding: utf-8 -*-
"""
Keep our favourite datasets from Quandl up-to-date.
This code is a working example.
author: Fabio Pani <fabiux@fabiopani.it>
"""
import Quandl
from pymongo import MongoClient
from datetime import datetime, timedelta

db = MongoClient().bitcoin  # our database: we're interested in BTC here... :-)

# config datasets here: one dict for each dataset
# label: Quandl code of the dataset
# collection: MongoDB collection for the dataset (db.<collection_name>)
# series: dict containing key-value pairs for data series in the database, where
# - key: name of the MongoDB collection field
# - value: name of the data series in data framework from Quandl
datasets = [dict(label='COINBASE/USD', collection=db.coinbase,
                 series=dict(open='Open', high='High', low='Low', volume='Volume')),
            dict(label='BITCOINWATCH/MINING', collection=db.mining,
                 series=dict(totalbtc='Total BTC', marketcap='Market Cap', translast24h='Transactions last 24h',
                             transavgperhour='Transactions avg. per hour', btcsentlast24h='Bitcoins sent last 24h',
                             btcsentavgperhour='Bitcoins sent avg. per hour', count='Count',
                             blockslast24h='Blocks last 24h', blocksavgperhour='Blocks avg. per hour',
                             difficulty='Difficulty', nextdifficulty='Next Difficulty',
                             nethashrateths='Network Hashrate Terahashs',
                             nethashratepflops='Network Hashrate PetaFLOPS'))]

now_date = str(datetime.now())[:10]  # current date (YYYY-MM-DD)


def get_last_date(collection):
    """
    Get last update date.
    :param collection: MongoDB collection for current dataset
    :type collection: object
    :return: (str) last update date or None if not found
    """
    # FIXME improve this...
    res = collection.find({}, {'_id': 1}).sort('_id', -1).limit(1)
    if res is None:
        return None
    else:
        maxdate = None
        for item in res:
            maxdate = item['_id']
            break
    return maxdate


def add_one_day(date):
    """
    Add one day to input date.
    :param date: input date (YYYY-MM-DD)
    :type date: str
    :return: (str) date of the day after (YYYY-MM-DD) or None if date is None
    """
    return None if date is None \
        else str(datetime(int(date[:4]), int(date[5:7]), int(date[8:10])) + timedelta(days=1))[:10]


def insert_data(df, series, collection):
    """
    Add data from Quandl framework to the corresponding MongoDB collection.
    :param df: data framework (Pandas)
    :type df: object
    :param series: mapping between MongoDB collection fields and Quandl data framework
    :type series: dict
    :param collection: MongoDB collection
    :type collection: object
    """
    for d in df.index:
        doc = dict()
        doc['_id'] = str(d)[:10]
        for k, v in series.items():
            doc[k] = df[v][d]
        collection.insert_one(doc)


def main():
    for dataset in datasets:
        last_date = get_last_date(dataset['collection'])
        if last_date < now_date:
            df = Quandl.get(dataset['label'], trim_start=add_one_day(last_date))  # all data if trim_start is None
            insert_data(df, dataset['series'], dataset['collection'])

if __name__ == '__main__':
    main()
