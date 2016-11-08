from pymongo import MongoClient
import logging


def connect_to_uri(mongo_connection_uri=None, logfile=None):
    logging.info("\nUpdating Mongo db\n")
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

    return client


def connect_to_db(client, database_name=None):

    logging.info('connecting to db')
    try:
        db = client[database_name]
    except Exception as e:
        logging.exception(e)

    return db


def get_collection(db, collection_name=None):

    logging.info('connecting to collection')
    try:
        coll = db[collection_name]
    except Exception as e:
        logging.exception(e)

    return coll


def get_event_names(coll):
    return list(coll.find({}, {'properties.Event':1}))
