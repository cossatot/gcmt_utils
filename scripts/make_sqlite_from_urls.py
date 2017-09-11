import matplotlib as mpl
mpl.use('svg')

from urllib.request import urlopen
import sqlite3 as lite

import sys; sys.path.append('../')
import gcmt_utils.ndk_parser as ndk
import gcmt_utils.sql_utils as sq
import gcmt_utils as gc

jan76_dec2013_ndk_url = 'http://www.ldeo.columbia.edu/~gcmt/projects/CMT/catalog/jan76_dec13.ndk'

base_monthly_url = ('http://www.ldeo.columbia.edu/~gcmt/projects/CMT/'
                    + 'catalog/NEW_MONTHLY/{}/{}.ndk')

years = ['2014', '2015', '2016']
months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
          'jul', 'aug', 'sep', 'oct', 'nov', 'dec']

def format_url(year, mo):
    yr_mo_string = mo + year[2:]

    return base_monthly_url.format(year, yr_mo_string)

monthly_url_list = [format_url(yr, mo) for mo in months for yr in years]


print('getting data from big catalog url')
jan_76_dec13_ndk_string = urlopen(jan76_dec2013_ndk_url).read().decode('utf-8')


print('parsing data')
eq_dict_list = ndk.parse_ndk_string(jan_76_dec13_ndk_string)

print('getting monthly data')

mo_data_list = []

for mo_url in monthly_url_list:
    try:
        mo_data = urlopen(mo_url).read().decode('utf-8')
        mo_data_list.append(mo_data)

    except: #HTTPError:
        pass

print('parsing monthly data')
for mo_string in mo_data_list:
    eq_dict_list += ndk.parse_ndk_string(mo_string)

print('adding beachballs to dicts')

bb_rel_dir_path = '../data/bbs/'


def get_bb(eq_d, bb_dir_path):

    cmt_name = eq_d['cmt_event_name']

    bb_path = bb_dir_path + cmt_name + '.png'
    
    try:
        with open(bb_path, 'rb') as f:
            img = f.read()
    except:
        img = 'NULL'
    eq_d.update({'focal_mech' : img})



print('doing SQL stuff')

gcmt_db = '../data/gcmt_table.sqlite'
gcmt_table = 'GCMT_events'
gcmt_schema = sq.sqlite_gcmt_table_schema

sq.make_gcmt_table(gcmt_table, gcmt_schema, gcmt_db)

eq_list_tuple = sq.make_big_table_tuple(eq_dict_list)

con = lite.connect(gcmt_db)

with con:
    sq.insert_many_rows(con, gcmt_table, len(sq.table_cols), eq_list_tuple)

print('done')
