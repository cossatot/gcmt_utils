#!/home/ec2-user/miniconda3/bin/python

import re
import logging

import boto3

import sys; sys.path.append('../')
import gcmt_utils as gc
from update_mongo_db import update_mongo_db


# connect to db
mc_uri = None
database_name = 'gcmt_dev'
collection_name='quakes'
logfile = './logs/mongo_update.log'
aws_profile_name = 'gcmt_viewer'
bucket='earth-analysis.com'
prefix = 'gcmt_viewer/data/beachballs/png_reduced/'

def get_bbs(client, bucket, prefix=None, strip_prefix=True):
    bucket_dict = client.list_objects(Bucket=bucket,
                                      Prefix=prefix,
                                      )
    
    paginator = client.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    contents = []

    for page in page_iterator:
        if 'Contents' in page:
            for key in page['Contents']:
                contents.append(key['Key'])

    if strip_prefix == True:
        contents = [re.sub('{}'.format(prefix), '', c) for c in contents]
    return contents


def compare_bb_lists(mongo_list, aws_list):
    return list(set(mongo_list) - set(aws_list))


def upload_bb(event=None, client=None, directory='./', fig_format='.png', 
              bucket=None, prefix=''):
    key = prefix + event.Event + fig_format
    filename = directory + '/' + event.Event + fig_format

    client.upload_file(Key=key, Bucket=bucket, Filename=filename)


def update_mongo_aws():
    new_cmts = update_mongo_db(mongo_connection_uri=mc_uri,
                               database_name=database_name,
                               collection_name=collection_name,
                               logfile=logfile,
                               return_new_event_list=True)
    
    logging.basicConfig(filename=logfile, 
                        level=logging.INFO,
                        format='%(asctime)s %(message)s'
                        )
    
    logging.info('{} new cmts'.format(len(new_cmts)))

    client = gc.connect_to_uri(mongo_connection_uri=mc_uri,
                               logfile=logfile)
    
    db = gc.connect_to_db(client, database_name=database_name)
    coll = gc.get_collection(db, collection_name=collection_name)

    events = list(coll.find({}, {'properties.Event':1}))
    #mongo_event_names = [ev['properties']['Event'] for ev in events]
    mongo_event_names = [ev.Event for ev in new_cmts]
    
    # get list of bbs on aws
    logging.info('getting existing BB list from AWS')
    try:
        session = boto3.Session(profile_name=aws_profile_name)
    except Exception as e:
        logging.exception(e)

    try:
        aws_client = session.client('s3')
    except Exception as e:
        logging.exception(e)

    bbs = get_bbs(aws_client, bucket='earth-analysis.com',
                  prefix='gcmt_viewer/data/beachballs/png_reduced/')

    logging.info('done fetching new BBs.')

    aws_bb_events = [bb.split('.')[0] for bb in bbs]

    # find events that have no bbs
    logging.info('finding new events that need BBs')
    aws_need_bbs = compare_bb_lists(mongo_event_names, aws_bb_events)
    logging.info('need {} new BBs'.format(len(aws_need_bbs)))


    if len(new_cmts) > 0 and len(aws_need_bbs) > 0:
        logging.info('making new BBs')
        
        new_event_counter = 0
        for event in new_cmts:
            if event.Event in aws_need_bbs:
                new_event_counter += 1
                logging.info('doing BB {} / {}'.format(new_event_counter,
                                                       len(aws_need_bbs)))
                gc.make_beachball(event, directory='bbs')
                gc.resize_bb_file(event, directory='bbs')
                try:
                    logging.info('uploading {}'.format(event.Event))
                    upload_bb(event=event, client=aws_client, directory='bbs',
                              prefix=prefix, bucket=bucket)
                except Exception as e:
                    logging.exception(e)
            else:
                pass

    else:
        logging.info('no new BBs needed')



if __name__ == '__main__':
    update_mongo_aws()
