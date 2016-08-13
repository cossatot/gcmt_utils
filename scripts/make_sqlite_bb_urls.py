from urllib.request import urlopen
import sqlite3 as sq

import sys; sys.path.append('../')
import gcmt_utils.ndk_parser as ndk
import gcmt_utils.db_utils as db

jan76_dec2013_ndk_url = ('http://www.ldeo.columbia.edu/~gcmt/projects/CMT/'
                         + 'catalog/jan76_dec13.ndk')

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

bb_base_url = ("http://earth-analysis.com/gcmt_viewer/data/beachballs"
               + "/png_reduced/{}.png")

def get_bb_url(eq_d, bb_base_url):

    cmt_name = eq_d['cmt_event_name']

    bb_url = bb_base_url.format(cmt_name)
    
    eq_d.update({'focal_mech' : bb_url})


for eq_d in eq_dict_list:
    get_bb_url(eq_d, bb_base_url)

print('doing SQL stuff')

gcmt_db = '../data/gcmt_table_bb_urls.sqlite'
gcmt_table = 'GCMT_events'
gcmt_schema = db.sqlite_gcmt_table_schema

db.make_gcmt_table(gcmt_table, gcmt_schema, gcmt_db)

eq_list_tuple = db.make_big_table_tuple(eq_dict_list)

con = sq.connect(gcmt_db)

with con:
    db.insert_many_rows(con, gcmt_table, len(db.table_cols), eq_list_tuple)

print('done')
