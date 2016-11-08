from urllib.request import urlopen
import time
import logging
import argparse
from pymongo import MongoClient

import sys; sys.path.append('../')
import gcmt_utils as gc

parser = argparse.ArgumentParser(argument_default=argparse.SUPPRESS)
parser.add_argument('-mc', '--mongo_connection_uri', required=False)
parser.add_argument('-db', '--database_name', required=False)
parser.add_argument('-c', '--collection_name', required=False)
parser.add_argument('-l', '--logfile', required=False)


def make_mongo_db(mongo_connection_uri=None, database_name='gcmt_dev',
                  collection_name='quakes', logfile='./logs/mongo_build.log'):

    logging.basicConfig(filename=logfile, 
                        level=logging.INFO,
                        format='%(asctime)s %(message)s'
                        )
    
    logging.info('\n Making new Mongo database.\n')
    t0 = time.time()
    
    logging.info("initializing mongo client")
    if mongo_connection_uri:
        try:
            client = MongoClient(mongo_connection_uri)
        except Exception as e:
            logging.exception(e)
    else:
        try:
            client = MongoClient()
        except Exception as e:
            logging.exception(e)

    logging.info('making new db')
    try:
        db = client[database_name]
    except Exception as e:
        logging.exception(e)

    logging.info('Making new collection')
    if collection_name in db.collection_names():
        logging.info('Collection {} already exists; deleting it.'.format(
                                                              collection_name))
        coll = db[collection_name]
        coll.drop()
    
    coll = db[collection_name]

    logging.info('processing ndk catalog')
    eq_list = gc.process_catalog_ndks(monthlies=False)
    gc.add_min_zoom(eq_list, bin_size_degrees=1.5)

    logging.info('inserting earthquakes in new collection')
    try:
        insert_results = coll.insert_many([eq.to_feature_dict() 
                                           for eq in eq_list])
    except Exception as e:
        logging.exception(e)

    logging.info('inserted {} new records'.format(
                                             len(insert_results.inserted_ids)))
    
    logging.info('closing mongo client')
    client.close()

    t1 = time.time()
    logging.info('Done making database in {:0.1f} s\n'.format(t1-t0))

if __name__ == '__main__':
    args = vars(parser.parse_args())
    make_mongo_db(**args)
