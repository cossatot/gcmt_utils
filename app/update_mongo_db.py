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


def update_mongo_db(mongo_connection_uri=None, 
                    database_name='gcmt_dev',
                    collection_name='quakes', 
                    logfile='./logs/mongo_update.log',
                    return_new_event_list=False,
                    ):

    t0 = time.time()

    logging.basicConfig(filename=logfile, 
                        level=logging.INFO,
                        format='%(asctime)s %(message)s'
                        )
   
    client = gc.connect_to_uri(mongo_connection_uri=mongo_connection_uri,
                               logfile=logfile)
    
    db = gc.connect_to_db(client, database_name=database_name)
    
    coll = gc.get_collection(db, collection_name=collection_name)

    
    logging.info('Pulling existing events')
    existing_eqs = list(coll.find({}, {'properties.Datetime' : 1,
                                       'properties.Mw' : 1,
                                       'properties.minZoom' : 1,
                                       'properties.Event' : 1,
                                       'geometry.coordinates' : 1,
                                       '_id' : 1} ))

    names = [ee['properties']['Event'] for ee in existing_eqs] # don't need?
    dates = [ee['properties']['Datetime'] for ee in existing_eqs]
    min_zooms = [ee['properties']['minZoom'] for ee in existing_eqs]
    ids = [ee['_id'] for ee in existing_eqs]
    last_date = sorted(dates)[-1]

    logging.info('Processing Quick CMTs')
    quick_cmt_list = gc.process_quick_cmts()
    
    #new_quick_cmts = [eq for eq in quick_cmt_list if eq.Datetime > last_date]
    new_quick_cmts = quick_cmt_list
    logging.info('{} new Quick CMTs'.format(len(new_quick_cmts)))
    #gc.make_beachballs(new_cmts)
    
    logging.info('Getting newer monthly CMTs')
    monthly_cmts = gc.process_catalog_ndks(base=False)
    new_monthly_cmts = [cmt for cmt in monthly_cmts if cmt.Event not in names]
    new_cmts = new_quick_cmts + new_monthly_cmts

    old_eqs = [gc.GCMT_event.from_feature_dict(ee) for ee in existing_eqs]
    all_eqs = old_eqs + new_cmts

    logging.info('Recalculating minZoom')
    gc.add_min_zoom(all_eqs, bin_size_degrees=1.5)

    logging.info('Updating minZooms for existing events')
    updated_min_zooms = [oeq.minZoom for oeq in old_eqs]

    updated_records = [(lambda i: ({'_id': ids[i]}, 
                                   {'$set': 
                                    {'properties.minZoom': new_min_zoom}}))(i)
                       for i, new_min_zoom in enumerate(updated_min_zooms)
                       if new_min_zoom != min_zooms[i]]

    update_record_results = [coll.update_one(*rc) for rc in updated_records]

    good_updates = 0
    bad_updates = []
    for i, ur in enumerate(update_record_results):
        if ur.raw_result['ok'] == 1:
            good_updates += 1
        else:
            bad_updates.append(ids[i])
    logging.info('Successfully updated {} records'.format(good_updates))
    logging.info('{} unsuccessful updates: {}'.format(len(bad_updates),
                                                      bad_updates))

    logging.info('Adding new events')
    try:
        insert_results = coll.insert_many([eq.to_feature_dict() 
                                           for eq in new_cmts])
    except Exception as e:
        logging.exception(e)

    logging.info('closing mongo client')
    client.close()

    t1 = time.time()
    logging.info('Done updating database in {:0.1f} s\n'.format(t1-t0))

    if return_new_event_list == True:
        return new_cmts


if __name__ == '__main__':
    args = vars(parser.parse_args())
    update_mongo_db(**args)
