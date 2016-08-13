from urllib.request import urlopen
import sqlite3 as sq

import sys; sys.path.append('../')
import gcmt_utils.ndk_parser as ndk
import gcmt_utils.db_utils as db


quick_cmt_url = ('http://www.ldeo.columbia.edu/~gcmt/projects/CMT/catalog/'
                 + 'NEW_QUICK/qcmt.ndk')

print('downloading new quick cmts')
quick_cmt_ndk = urlopen(quick_cmt_url).read().decode('utf-8')


'''
need to figure out how to identify cmts from quick cmts already in the 
db, then remove those and replace with new monthlies
'''

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


print('inserting new events in database')
# check to see if event is in db already

def check_old_events(event_name, con):
    old_prefixes = ('B', 'C', 'D', 'G', 'L', 'M', 'S', 'Z')

    _event_name = event_name[1:]

    return any(db.check_exists('Event', op+_event_name, 'GCMT_events', con)
               for op in old_prefixes)

gcmt_db = '../data/gcmt_table_bb_urls.sqlite'
gcmt_table = 'GCMT_events'
gcmt_schema = db.sqlite_gcmt_table_schema



with sq.connect(gcmt_db) as con:
    new_count = 0
    for eq_d in quick_cmt_list:
        if not check_old_events(eq_d['cmt_event_name'], con):
            cur = con.cursor()
            r_tup = db.make_row_tuple(eq_d)
            db.insert_row_tuple(cur, gcmt_table, r_tup)

            new_count += 1

print('inserted {} new earthquakes'.format(new_count))

