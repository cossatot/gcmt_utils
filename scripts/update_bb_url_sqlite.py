from urllib.request import urlopen
import sqlite3 as sq
import time

import sys; sys.path.append('../')
import gcmt_utils.ndk_parser as ndk
import gcmt_utils.sql_utils as db

t0 = time.time()

quick_cmt_url = ('http://www.ldeo.columbia.edu/~gcmt/projects/CMT/catalog/'
                 + 'NEW_QUICK/qcmt.ndk')

print('downloading new quick cmts')
quick_cmt_ndk = urlopen(quick_cmt_url).read().decode('utf-8')

print('parsing quick cmts')

quick_cmt_list = ndk.parse_ndk_string(quick_cmt_ndk)

bb_base_url = ("http://earth-analysis.com/gcmt_viewer/data/beachballs"
               + "/png_reduced/{}.png")

def get_bb_url(eq_d, bb_dir_path):

    cmt_name = eq_d['cmt_event_name']

    bb_url = bb_base_url.format(cmt_name)
    
    eq_d.update({'focal_mech' : bb_url})

for eq_d in quick_cmt_list:
    get_bb_url(eq_d, bb_base_url)


catalog_prefixes = ('B', 'C', 'D', 'G', 'L', 'M', 'S', 'Z')

print('inserting new events in database')
gcmt_db = '../data/gcmt_table_bb_urls.sqlite'
gcmt_table = 'GCMT_events'
gcmt_schema = db.sqlite_gcmt_table_schema


new_row_list = []
with sq.connect(gcmt_db) as con:
    new_event_names = [eq_d['cmt_event_name'] for eq_d in quick_cmt_list]

    events_in_db = db.check_event_exists(vals=new_event_names, tag='Event',
                                         table=gcmt_table, con=con)

    new_row_tup = tuple(db.make_row_tuple(eq_d)
                        for i, eq_d in enumerate(quick_cmt_list)
                        if not events_in_db[i])

    db.insert_many_rows(con, gcmt_table, len(new_row_tup[0]),
                        new_row_tup)

t1 = time.time()

print('inserted {} Quick CMT earthquakes in {} s'.format(len(new_row_tup), 
                                                           int(t1-t0)))


